///|
/// Encode all fragments and check every budget before mutating counters or queues.
fn Endpoint::prepare_delivery(
  self : Endpoint,
  session : SessionState,
  handle : UInt32,
  message : Message,
  tag : Bytes,
  settled~ : Bool = false,
) -> (UInt32, Array[Bytes], Int32, Bytes) raise AmqpError {
  let link = session.local_link(handle)
  if !link.sender || !link.ready || link.credit == 0U {
    raise State("sender not ready or link credit exhausted")
  }
  if link.sent_incomplete || link.peer_incomplete {
    raise State("new delivery requires complete unsettled maps after reattach")
  }
  if tag.length() > 32 {
    raise Invalid("delivery tag length", 0)
  }
  if (link.settle_mode == 0 && settled) || (link.settle_mode == 1 && !settled) {
    raise State(
      "delivery settlement disagrees with negotiated sender settle mode",
    )
  }
  if !settled &&
    (
      session.outgoing.length() >= self.limits.max_pending ||
      link.records.length() >= self.limits.max_pending
    ) {
    raise Limit("outgoing unsettled deliveries")
  }
  if link.records.contains(tag) {
    raise State("delivery-tag already unsettled on this link")
  }
  for (pending_id, pending_handle) in session.outgoing {
    if pending_handle == handle && session.outgoing_tags[pending_id] == tag {
      raise State("delivery-tag already unsettled on this link")
    }
  }
  let bytes = encode_message(message, limits=self.limits)
  if link.peer_max_message != 0UL &&
    bytes.length().to_uint64() > link.peer_max_message {
    raise Limit("peer max-message-size")
  }
  if !settled && link.recoverable {
    self.check_retained_bytes(bytes.length())
  }
  let id = session.next_delivery
  if session.outgoing.contains(id) {
    raise State("delivery ID wrap collides with unsettled delivery")
  }
  let (frames, total) = self.prepare_transfers(
    session, handle, id, tag, bytes, settled,
  )
  (id, frames, total, bytes)
}

///|
fn Endpoint::prepare_transfers(
  self : Endpoint,
  session : SessionState,
  handle : UInt32,
  id : UInt32,
  tag : Bytes,
  bytes : Bytes,
  settled : Bool,
  resuming~ : Bool = false,
  state~ : Value = Null,
  aborted~ : Bool = false,
) -> (Array[Bytes], Int32) raise AmqpError {
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
    if first && resuming {
      fields.push(Null)
      fields.push(state)
      fields.push(Boolean(true))
      if aborted {
        fields.push(Boolean(true))
      }
    } else if resuming {
      fields.push(Null)
      fields.push(Null)
      fields.push(Boolean(true))
    }
    let overhead = self.frame(0x14, fields, channel=session.channel).length()
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
      channel=session.channel,
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
  if !session.window.can_send(frames=frames.length()) {
    raise State(
      "session window cannot fit complete delivery; refresh flow before retry",
    )
  }
  if total > self.limits.max_value_bytes - self.io.queued_bytes ||
    frames.length() > self.limits.max_items - self.io.outbound.length() {
    raise Limit("outbound queue; drain before send")
  }
  (frames, total)
}

///|
/// Encode and validate a delivery without consuming credit or modifying queues.
pub fn Endpoint::plan_send(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  handle : UInt32,
  message : Message,
  tag : Bytes,
  settled~ : Bool = false,
) -> SendPlan raise AmqpError {
  let session = self.session_active(channel)
  let (_, frames, bytes, _) = self.prepare_delivery(
    session,
    handle,
    message,
    tag,
    settled~,
  )
  {
    frames: frames.length(),
    wire_bytes: bytes,
    link_credit: session.local_link(handle).credit,
  }
}

///|
#derive(Debug)
pub(all) struct SendPlan {
  frames : Int32
  wire_bytes : Int32
  link_credit : UInt32
}

///|
pub extend SendPlan with @debug.Debug::{to_repr}

///|
pub fn Endpoint::send(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  handle : UInt32,
  message : Message,
  tag : Bytes,
  settled~ : Bool = false,
) -> UInt32 raise AmqpError {
  let session = self.session_active(channel)
  let (id, frames, total, bytes) = self.prepare_delivery(
    session,
    handle,
    message,
    tag,
    settled~,
  )
  let link = session.local_link(handle)
  // All operations below are non-failing after the preflight.
  self.commit_transfers(session, link, frames, total)
  if !settled {
    session.outgoing[id] = handle
    session.outgoing_tags[id] = tag
    if link.recoverable {
      link.records[tag] = {
        id: Some(id),
        state: Null,
        bytes,
        delivered: false,
      }
      session.recovering_outgoing[id] = (link, tag)
    }
  }
  id
}

///|
pub fn Endpoint::pending(self : Endpoint) -> (Int32, Int32) {
  (self.primary.outgoing.length(), self.primary.incoming.length())
}

///|
fn Endpoint::commit_transfers(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  frames : Array[Bytes],
  total : Int32,
) -> Unit raise AmqpError {
  session.window.sent(frames=frames.length())
  for frame in frames {
    self.io.outbound.push(frame)
  }
  self.io.queued_bytes += total
  link.credit -= 1U
  link.delivery_count += 1U
  session.next_delivery += 1U
}
