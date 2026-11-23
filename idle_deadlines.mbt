///|
/// Return remaining time for receive timeout and outbound heartbeat respectively.
pub fn Endpoint::idle_deadlines(
  self : Endpoint,
  now_ms : UInt64,
) -> (UInt64?, UInt64?) raise AmqpError {
  if self.closed || self.failed || self.io.closing || !self.remote_open {
    return (None, None)
  }
  guard self.clock_ms is Some(previous) else {
    raise State("idle scheduling requires receive_at")
  }
  if now_ms < previous {
    raise State("idle clock moved backwards")
  }
  let rx = if self.idle_advertised_ms == 0U {
    None
  } else {
    Some(
      idle_remaining(
        self.idle_advertised_ms.to_uint64() * 2UL,
        now_ms - self.last_rx_ms,
      ),
    )
  }
  let tx = if self.peer_idle_ms == 0U || self.io.queued_bytes != 0 {
    None
  } else {
    Some(
      idle_remaining(
        heartbeat_interval(self.peer_idle_ms),
        now_ms - self.last_tx_ms,
      ),
    )
  }
  (rx, tx)
}
