///|
fn Endpoint::receive_transfer(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
  payload : Bytes,
) -> Unit raise AmqpError {
  let link = session.remote_link(uint_field(perf, 0, 0U))
  if link.sender || !link.ready {
    raise State("transfer requires attached receiver")
  }
  if uint_field(perf, 3, 0U) != 0U {
    raise Unsupported("nonzero message-format")
  }
  if bool_field(perf, 8, false) || !(perf.field(7) is Null) {
    raise Unsupported("resumed or stateful transfer")
  }
  let more = bool_field(perf, 5, false)
  let aborted = bool_field(perf, 9, false)
  if aborted && more {
    raise Invalid("aborted transfer also has more", 0)
  }
  let partial = match link.partial {
    Some(partial) => {
      match perf.field(1) {
        Null => ()
        UInt(n) if n == partial.id => ()
        _ => raise Invalid("continuation delivery-id", 0)
      }
      match perf.field(2) {
        Null => ()
        Binary(tag) if tag == partial.tag => ()
        _ => raise Invalid("continuation delivery-tag", 0)
      }
      match perf.field(4) {
        Null => ()
        Boolean(n) if n == partial.settled => ()
        _ => raise Unsupported("settlement change during fragmented delivery")
      }
      if !partial.settled {
        match perf.field(6) {
          Null => ()
          UByte(0) if partial.receiver_mode == First => ()
          UByte(1) if partial.receiver_mode == Second => ()
          _ =>
            raise Unsupported(
              "receiver settlement mode changed during delivery",
            )
        }
      }
      partial
    }
    None => {
      if link.credit == 0U {
        raise State("remote transfer exceeded link credit")
      }
      let id = match perf.field(1) {
        UInt(n) => n
        _ => raise Invalid("first transfer requires delivery-id", 0)
      }
      let tag = match perf.field(2) {
        Binary(b) => b
        _ => raise Invalid("first transfer requires delivery-tag", 0)
      }
      match session.remote_next_delivery {
        Some(expected) if id != expected =>
          raise Invalid("non-sequential session delivery-id", 0)
        _ => ()
      }
      let settled = bool_field(perf, 4, false)
      if session.incoming.contains(id) {
        raise Invalid("delivery-id wrap collides with unsettled delivery", 0)
      }
      if (link.settle_mode == 0 && settled) ||
        (link.settle_mode == 1 && !settled) {
        raise Invalid(
          "transfer settlement disagrees with sender settle mode", 0,
        )
      }
      if !settled &&
        (
          session.incoming.contains(id) ||
          session.incoming.length() >= self.limits.max_pending
        ) {
        raise Limit("incoming unsettled deliveries")
      }
      for (pending_id, pending_handle) in session.incoming {
        if pending_handle == link.local_handle &&
          session.incoming_tags[pending_id] == tag {
          raise Invalid("duplicate unsettled delivery-tag on receiver link", 0)
        }
      }
      let receiver_mode = if settled {
        First
      } else {
        match perf.field(6) {
          Null => link.receiver_mode
          UByte(0) => First
          UByte(1) if link.receiver_mode == Second => Second
          _ =>
            raise Invalid("transfer receiver settle mode exceeds link mode", 0)
        }
      }
      let partial : PartialDelivery = {
        id,
        tag,
        settled,
        receiver_mode,
        bytes: [],
        fragments: 0,
      }
      link.credit -= 1U
      link.delivery_count += 1U
      session.remote_next_delivery = Some(id + 1U)
      link.partial = Some(partial)
      if !settled {
        session.incoming[id] = link.local_handle
        session.incoming_tags[id] = tag
        if receiver_mode == First {
          session.incoming_first.add(id)
        }
      }
      partial
    }
  }
  session.window.received()
  if partial.fragments >= self.limits.max_fragments ||
    payload.length() > self.limits.max_message_bytes - partial.bytes.length() {
    raise Limit("incoming fragmented message")
  }
  partial.fragments += 1
  if aborted {
    session.forget_incoming(partial.id)
    self.partial_bytes -= partial.bytes.length()
    link.partial = None
    self.session_event(session, Aborted(link.local_handle, partial.id))
    self.complete_received_drain(session, link)
    return
  }
  if payload.length() >
    self.limits.max_value_bytes - self.partial_bytes - self.event_bytes {
    raise Limit("aggregate incoming payload budget; drain events")
  }
  for byte in payload {
    partial.bytes.push(byte)
  }
  self.partial_bytes += payload.length()
  if !more {
    let message = decode_message(
      Bytes::from_array(partial.bytes),
      limits=self.limits,
    )
    link.partial = None
    self.partial_bytes -= partial.bytes.length()
    self.event_bytes += partial.bytes.length()
    self.session_event(
      session,
      Received({
        handle: link.local_handle,
        id: partial.id,
        tag: partial.tag,
        settled: partial.settled,
        message,
      }),
    )
    self.complete_received_drain(session, link)
  }
}
