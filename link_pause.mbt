///|
fn SessionState::check_quiescent(
  self : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if link.partial is Some(_) ||
    link.draining ||
    self.incoming.values().any(h => h == link.local_handle) ||
    self.outgoing.values().any(h => h == link.local_handle) {
    raise Unsupported(
      "suspension requires no unsettled, partial or draining delivery",
    )
  }
}

///|
/// Detach without closing a quiescent link, preserving its identity and counter.
pub fn Endpoint::suspend_link(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  if !link.ready || (!link.sender && link.credit != 0U) {
    raise State("receiver must exhaust or drain credit before suspension")
  }
  session.check_quiescent(link)
  self.queue(self.frame(0x16, [UInt(handle), Boolean(false)], channel~))
  link.pause_pending = true
  link.ready = false
}

///|
fn Endpoint::receive_suspend(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if link.closed {
    raise State("cannot suspend a closed link")
  }
  session.check_quiescent(link)
  if !link.pause_pending {
    self.queue(
      self.frame(
        0x16,
        [UInt(link.local_handle), Boolean(false)],
        channel=session.channel,
      ),
    )
  }
  link.ready = false
  link.pause_pending = false
  link.suspended = true
  link.credit = 0U
  link.drain_mode = false
  link.drain_returned = 0U
  link.remote_handle = None
  self.session_event(session, LinkSuspended(link.name, link.local_handle))
}

///|
fn SessionState::resume_handle(
  self : SessionState,
  requested : UInt32?,
) -> UInt32 raise AmqpError {
  let handle = requested.unwrap_or_else(() => self.next_handle())
  if handle > self.peer_handle_max {
    raise Limit("resume peer handle-max")
  }
  if self.links.any(l => !l.suspended && l.local_handle == handle) {
    raise State("resume handle is already mapped")
  }
  handle
}

///|
/// Reattach the same local identity during the same live session. Fresh credit
/// is negotiated after LinkReady. Only empty unsettled maps are supported.
pub fn Endpoint::resume_link(
  self : Endpoint,
  name : String,
  handle? : UInt32,
  channel~ : Int32 = self.channel,
) -> UInt32 raise AmqpError {
  let session = self.session_active(channel)
  let link = session.links.iter().find_first(l => l.name == name && l.suspended)
  guard link is Some(link) else { raise State("unknown suspended link") }
  let handle = session.resume_handle(handle)
  let fields = link.attach_fields(self.limits)
  fields[1] = UInt(handle)
  fields[7] = MapValue([])
  self.queue(self.frame(0x12, fields, channel~))
  link.local_handle = handle
  link.suspended = false
  handle
}

///|
fn Endpoint::accept_resumed(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  perf : Performative,
) -> LinkState raise AmqpError {
  let handle = session.resume_handle(None)
  let previous = link.delivery_count
  link.attach(perf, responding=true)
  if link.delivery_count != previous {
    raise Invalid("resumed quiescent link counter differs", 0)
  }
  link.local_handle = handle
  link.suspended = false
  let fields = link.attach_fields(self.limits)
  fields[7] = MapValue([])
  self.queue(self.frame(0x12, fields, channel=session.channel))
  link
}
