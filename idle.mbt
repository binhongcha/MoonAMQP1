///|
#derive(Eq, Debug)
pub(all) enum IdleAction {
  Idle
  HeartbeatQueued
  IdleCloseQueued
}

///|
fn Endpoint::observe_clock(
  self : Endpoint,
  now : UInt64,
) -> Unit raise AmqpError {
  match self.clock_ms {
    Some(previous) if now < previous =>
      raise State("idle clock moved backwards")
    None => {
      self.last_rx_ms = now
      self.last_tx_ms = now
    }
    _ => ()
  }
  self.clock_ms = Some(now)
}

///|
/// Feed transport bytes. Only an accepted complete frame refreshes receive activity.
pub fn Endpoint::receive_at(
  self : Endpoint,
  bytes : Bytes,
  now_ms : UInt64,
) -> Unit raise AmqpError {
  self.active()
  self.observe_clock(now_ms)
  self.receive_impl(bytes)
}

///|
/// Pull bytes when the host can immediately hand them to its ordered transport.
pub fn Endpoint::drain_at(
  self : Endpoint,
  now_ms : UInt64,
) -> Bytes raise AmqpError {
  self.observe_clock(now_ms)
  let bytes = self.drain()
  if !bytes.is_empty() {
    self.last_tx_ms = now_ms
  }
  bytes
}

///|
/// The advertised receive interval is half the local failure threshold.
/// Hosts call this periodically and write queued output as soon as possible.
pub fn Endpoint::poll_idle(
  self : Endpoint,
  now_ms : UInt64,
) -> IdleAction raise AmqpError {
  if self.closed || self.failed || self.closing || !self.remote_open {
    return Idle
  }
  if self.clock_ms is None {
    raise State("idle polling requires receive_at")
  }
  self.observe_clock(now_ms)
  if self.idle_advertised_ms != 0U &&
    now_ms - self.last_rx_ms >= self.idle_advertised_ms.to_uint64() * 2UL {
    self.queue(
      self.frame(0x18, [
        Described(
          ULong(0x1dUL),
          List([
            Symbol("amqp:resource-limit-exceeded"),
            Text("connection idle timeout"),
          ]),
        ),
      ]),
    )
    self.closing = true
    return IdleCloseQueued
  }
  let peer = self.peer_idle_ms.to_uint64()
  if peer != 0UL &&
    self.queued_bytes == 0 &&
    now_ms - self.last_tx_ms >= (peer / 2UL).max(1UL) {
    self.heartbeat()
    HeartbeatQueued
  } else {
    Idle
  }
}

///|
pub extend IdleAction with Eq::{equal, not_equal}

///|
pub extend IdleAction with Debug::{to_repr}
