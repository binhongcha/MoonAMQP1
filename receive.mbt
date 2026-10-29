///|
fn Endpoint::receive_transfer(
  self : Endpoint,
  perf : Performative,
  payload : Bytes,
) -> Unit raise AmqpError {
  let link = self.remote_link(uint_field(perf, 0, 0U))
  if link.sender || !link.ready {
    raise State("transfer requires attached receiver")
  }
  if uint_field(perf, 3, 0U) != 0U {
    raise Unsupported("nonzero message-format")
  }
  if bool_field(perf, 8, false) || !(perf.field(7) is Null) {
    raise Unsupported("resumed or stateful transfer")
  }
  match perf.field(6) {
    Null | UByte(0) => ()
    _ => raise Unsupported("receiver settle mode second")
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
      match self.remote_next_delivery {
        Some(expected) if id != expected =>
          raise Invalid("non-sequential session delivery-id", 0)
        _ => ()
      }
      let settled = bool_field(perf, 4, false)
      if !settled &&
        (
          self.incoming.contains(id) ||
          self.incoming.length() >= self.limits.max_pending
        ) {
        raise Limit("incoming unsettled deliveries")
      }
      let partial : PartialDelivery = {
        id,
        tag,
        settled,
        bytes: [],
        fragments: 0,
      }
      link.credit -= 1U
      link.delivery_count += 1U
      self.remote_next_delivery = Some(id + 1U)
      link.partial = Some(partial)
      partial
    }
  }
  self.window.received()
  if partial.fragments >= self.limits.max_fragments ||
    payload.length() > self.limits.max_message_bytes - partial.bytes.length() {
    raise Limit("incoming fragmented message")
  }
  partial.fragments += 1
  if aborted {
    self.partial_bytes -= partial.bytes.length()
    link.partial = None
    self.event(Aborted(link.local_handle, partial.id))
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
    if !partial.settled {
      self.incoming[partial.id] = link.local_handle
    }
    link.partial = None
    self.partial_bytes -= partial.bytes.length()
    self.event_bytes += partial.bytes.length()
    self.event(
      Received({
        handle: link.local_handle,
        id: partial.id,
        tag: partial.tag,
        settled: partial.settled,
        message,
      }),
    )
  }
}
