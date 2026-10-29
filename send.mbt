///|
/// Encode all fragments and check every budget before mutating counters or queues.
pub fn Endpoint::send(
  self : Endpoint,
  handle : UInt32,
  message : Message,
  tag : Bytes,
  settled~ : Bool = false,
) -> UInt32 raise AmqpError {
  self.session_active()
  let link = self.local_link(handle)
  if !link.sender || !link.ready || link.credit == 0U {
    raise State("sender not ready or link credit exhausted")
  }
  if tag.length() > 32 {
    raise Invalid("delivery tag length", 0)
  }
  if (link.settle_mode == 0 && settled) || (link.settle_mode == 1 && !settled) {
    raise State(
      "delivery settlement disagrees with negotiated sender settle mode",
    )
  }
  if !settled && self.outgoing.length() >= self.limits.max_pending {
    raise Limit("outgoing unsettled deliveries")
  }
  let bytes = encode_message(message, limits=self.limits)
  if link.peer_max_message != 0UL &&
    bytes.length().to_uint64() > link.peer_max_message {
    raise Limit("peer max-message-size")
  }
  let id = self.next_delivery
  if self.outgoing.contains(id) {
    raise State("delivery ID wrap collides with unsettled delivery")
  }
  let frames : Array[Bytes] = []
  let mut offset = 0
  let mut total = 0
  while true {
    let first = frames.is_empty()
    let fields : Array[Value] = if first {
      [
        UInt(handle),
        UInt(id),
        Binary(tag),
        UInt(0U),
        Boolean(settled),
        Boolean(true),
      ]
    } else {
      [UInt(handle), Null, Null, Null, Null, Boolean(true)]
    }
    let overhead = self.frame(0x14, fields).length()
    let capacity = minimum(self.limits.max_frame_bytes, self.peer_frame_max) -
      overhead
    if capacity <= 0 {
      raise Limit("transfer fields exceed frame budget")
    }
    let count = minimum(bytes.length() - offset, capacity)
    let more = offset + count < bytes.length()
    fields[5] = Boolean(more)
    let frame = self.frame(
      0x14,
      fields,
      payload=bytes[offset:offset + count].to_owned(),
    )
    total = checked_add(total, frame.length())
    if frames.length() >= self.limits.max_fragments ||
      frames.length() >= self.limits.max_items {
      raise Limit("outgoing fragment count")
    }
    frames.push(frame)
    offset += count
    if !more {
      break
    }
  }
  if !self.window.can_send(frames=frames.length()) {
    raise State(
      "session window cannot fit complete delivery; refresh flow before retry",
    )
  }
  if total > self.limits.max_value_bytes - self.queued_bytes ||
    frames.length() > self.limits.max_items - self.outbound.length() {
    raise Limit("outbound queue; drain before send")
  }
  // All operations below are non-failing after the preflight.
  self.window.sent(frames=frames.length())
  for frame in frames {
    self.outbound.push(frame)
  }
  self.queued_bytes += total
  link.credit -= 1U
  link.delivery_count += 1U
  if !settled {
    self.outgoing[id] = handle
  }
  self.next_delivery += 1U
  id
}

///|
pub fn Endpoint::pending(self : Endpoint) -> (Int32, Int32) {
  (self.outgoing.length(), self.incoming.length())
}
