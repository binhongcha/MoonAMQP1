///|
/// Drain a prefix of queued wire packets that fits the transport budget.
/// An oversized head packet stays queued and is reported to the caller.
pub fn Endpoint::drain_bounded_at(
  self : Endpoint,
  max_bytes : Int32,
  now_ms : UInt64,
) -> Bytes raise AmqpError {
  if max_bytes <= 0 {
    raise State("transport batch limit must be positive")
  }
  if !self.outbound.is_empty() && self.outbound[0].length() > max_bytes {
    raise Limit("head packet exceeds transport batch limit")
  }
  self.observe_clock(now_ms)
  let mut count = 0
  let mut size = 0
  let output : Array[Byte] = []
  for packet in self.outbound {
    if packet.length() > max_bytes - size {
      break
    }
    for b in packet {
      output.push(b)
    }
    size += packet.length()
    count += 1
  }
  if count > 0 {
    let remaining = self.outbound[count:].to_owned()
    self.outbound.clear()
    for packet in remaining {
      self.outbound.push(packet)
    }
    self.queued_bytes -= size
    self.last_tx_ms = now_ms
  }
  Bytes::from_array(output)
}
