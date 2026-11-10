///|
fn Endpoint::flow_frame(
  self : Endpoint,
  session : SessionState,
  link : LinkState?,
  count~ : UInt32? = None,
  credit~ : UInt32? = None,
  draining~ : Bool? = None,
) -> Bytes raise AmqpError {
  let (incoming, outgoing, _) = session.window.remaining()
  let fields = [
    UInt(session.window.next_in),
    UInt(incoming),
    UInt(session.window.next_out),
    UInt(outgoing),
  ]
  match link {
    Some(link) => {
      fields.push(UInt(link.local_handle))
      fields.push(UInt(count.unwrap_or(link.delivery_count)))
      fields.push(UInt(credit.unwrap_or(link.credit)))
      if draining.unwrap_or(link.drain_mode) {
        fields.push(Null)
        fields.push(Boolean(true))
      }
    }
    None => ()
  }
  self.frame(0x13, fields, channel=session.channel)
}

///|
pub fn Endpoint::grant(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  handle : UInt32,
  credit : UInt32,
  drain~ : Bool = false,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  if link.sender || !link.ready {
    raise State("grant needs attached receiver link")
  }
  bounded_window(credit)
  if credit.to_uint64() > self.limits.max_pending.to_uint64() {
    raise Limit("credit above pending budget")
  }
  if drain && credit == 0U {
    raise State("drain requires positive credit")
  }
  if link.draining {
    raise State("credit update while drain is pending")
  }
  self.queue(
    self.flow_frame(
      session,
      Some(link),
      credit=Some(credit),
      draining=Some(drain),
    ),
  )
  link.credit = credit
  link.drain_mode = drain
  link.draining = drain
  link.drain_returned = 0U
}

///|
pub fn Endpoint::refresh_windows(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  incoming : UInt32,
  outgoing : UInt32,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let (old_in, old_out, _) = session.window.remaining()
  session.window.refresh(incoming, outgoing)
  errdefer (session.window.refresh(old_in, old_out) catch {
    _ => abort("window invariant")
  })
  self.queue(self.flow_frame(session, None))
}

///|
fn Endpoint::receive_flow(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
) -> Unit raise AmqpError {
  let next_in = match perf.field(0) {
    UInt(n) => Some(n)
    _ => Some(0U)
  }
  session.window.update_peer(
    next_in,
    uint_field(perf, 1, 0U),
    uint_field(perf, 2, 0U),
    uint_field(perf, 3, 0U),
  )
  let draining = bool_field(perf, 8, false)
  let echo_link = match perf.field(4) {
    Null => {
      if draining {
        raise Invalid("drain requires link handle", 0)
      }
      None
    }
    UInt(handle) => {
      let link = session.remote_link(handle)
      if !link.ready {
        raise State("flow before attach")
      }
      if link.sender {
        let count = uint_field(perf, 5, 0U)
        link.update_credit(count, uint_field(perf, 6, 0U))
        link.drain_mode = draining
        link.draining = draining
        self.session_event(session, Credit(link.local_handle, link.credit))
        if draining {
          self.session_event(
            session,
            DrainRequested(link.local_handle, link.credit),
          )
        }
      } else if draining {
        self.receive_drained(session, link, perf)
      }
      Some(link)
    }
    _ => raise Invalid("flow handle", 0)
  }
  if bool_field(perf, 9, false) {
    self.queue(self.flow_frame(session, echo_link))
  }
}

///|
/// After sending available messages, return unused link credit to the receiver.
pub fn Endpoint::complete_drain(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  if !link.sender || !link.draining {
    raise State("complete_drain requires a sender with a pending drain")
  }
  let next = link.delivery_count + link.credit
  self.queue(
    self.flow_frame(session, Some(link), count=Some(next), credit=Some(0U)),
  )
  link.delivery_count = next
  link.credit = 0U
  link.draining = false
}

///|
fn Endpoint::receive_drained(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  perf : Performative,
) -> Unit raise AmqpError {
  let count = match perf.field(5) {
    UInt(n) => n
    _ => raise Invalid("drain response requires delivery-count", 0)
  }
  let returned = serial_distance(link.delivery_count, count)
  let peer_credit = uint_field(perf, 6, 0U)
  if returned == 0U && (!link.draining || link.credit > 0U || peer_credit > 0U) {
    // An echo or a late empty Flow can report state without returning credit.
    return
  }
  if !link.draining || returned != link.credit || peer_credit != 0U {
    raise Invalid("drain response does not exhaust outstanding link credit", 0)
  }
  link.delivery_count = count
  link.credit = 0U
  // A sender can return unused credit between fragments of its last delivery.
  link.drain_returned += returned
  self.complete_received_drain(session, link)
}

///|
fn Endpoint::complete_received_drain(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if link.draining && link.credit == 0U && link.partial is None {
    link.draining = false
    self.session_event(
      session,
      DrainCompleted(link.local_handle, link.drain_returned),
    )
  }
}
