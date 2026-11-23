///|
/// Start a session on the selected local channel after exchanging Open.
pub fn Endpoint::begin(
  self : Endpoint,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  self.active()
  if !self.remote_open || self.io.closing {
    raise State("session begin requires open connection")
  }
  let session = self.new_session(channel)
  self.queue(self.begin_frame(session, Null))
  self.register_session(session)
}

///|
fn Endpoint::new_session(
  self : Endpoint,
  channel : Int32,
) -> SessionState raise AmqpError {
  require_range(channel, 0, self.peer_channel_max)
  if self.sessions.contains(channel) {
    raise State("local session channel already mapped")
  }
  if self.sessions.length() >= self.max_sessions {
    raise Limit("session count")
  }
  session_state(channel, self.default_window)
}

///|
fn Endpoint::register_session(self : Endpoint, session : SessionState) -> Unit {
  self.sessions[session.channel] = session
  if session.channel == self.channel {
    self.primary = session
  }
}

///|
fn Endpoint::begin_frame(
  self : Endpoint,
  session : SessionState,
  remote : Value,
) -> Bytes raise AmqpError {
  let (incoming, outgoing, _) = session.window.remaining()
  self.frame(
    0x11,
    [remote, UInt(session.window.next_out), UInt(incoming), UInt(outgoing)],
    channel=session.channel,
  )
}

///|
fn Endpoint::receive_begin(
  self : Endpoint,
  remote_channel : Int32,
  perf : Performative,
) -> Unit raise AmqpError {
  if self.remote_sessions.contains(remote_channel) {
    raise Invalid("duplicate remote session channel", 0)
  }
  let session = match perf.field(0) {
    UShort(channel) =>
      match self.sessions.get(channel) {
        Some(session) if session.remote_channel is None => session
        _ =>
          raise Invalid(
            "remote-channel does not map a pending local session", 0,
          )
      }
    Null => {
      let preferred = self.channel
      let channel = if preferred <= self.peer_channel_max &&
        !self.sessions.contains(preferred) {
        preferred
      } else {
        let mut free = 0
        while free <= self.peer_channel_max && self.sessions.contains(free) {
          free += 1
        }
        free
      }
      let session = self.new_session(channel)
      self.queue(self.begin_frame(session, UShort(remote_channel)))
      self.register_session(session)
      session
    }
    _ => raise Invalid("remote-channel", 0)
  }
  session.window.establish(
    uint_field(perf, 1, 0U),
    uint_field(perf, 2, 0U),
    uint_field(perf, 3, 0U),
  )
  session.remote_channel = Some(remote_channel)
  session.peer_handle_max = uint_field(perf, 4, 4294967295U)
  self.remote_sessions[remote_channel] = session
  self.session_event(session, SessionReady)
}

///|
pub fn Endpoint::receive(
  self : Endpoint,
  bytes : Bytes,
) -> Unit raise AmqpError {
  if self.idle_advertised_ms != 0U || self.clock_ms is Some(_) {
    raise State("timed connection requires receive_at")
  }
  self.receive_impl(bytes)
}

///|
fn Endpoint::receive_impl(
  self : Endpoint,
  bytes : Bytes,
) -> Unit raise AmqpError {
  self.active()
  errdefer self.fail_connection()
  let waiting_for_header = self.stream.header_needed
  let items = self.stream.feed(bytes) catch {
    Unsupported(_) if waiting_for_header && self.stream.header_needed => {
      let send_header = !self.started || self.io.queued_bytes != 0
      self.fail_connection()
      self.closed = true
      if send_header {
        self.queue(protocol_header())
      }
      return
    }
    error => raise error
  }
  for item in items {
    match item {
      Header => if !self.started { self.start() }
      AmqpFrame(frame) => {
        self.receive_frame(frame)
        if self.clock_ms is Some(now) {
          self.last_rx_ms = now
        }
      }
    }
  }
}

///|
fn Endpoint::receive_frame(
  self : Endpoint,
  frame : Frame,
) -> Unit raise AmqpError {
  if self.closed {
    raise State("frame after connection close")
  }
  let perf = match frame.performative {
    None => {
      if !self.remote_open {
        raise State("empty frame before Open")
      }
      return
    }
    Some(perf) => perf
  }
  if self.io.closing && perf.code != 0x18 {
    // The peer may have sent this frame before it received our Close.
    return
  }
  if perf.code == 0x10 {
    if frame.channel != 0 || self.remote_open {
      raise Invalid("duplicate or nonzero-channel open", 0)
    }
    let idle = uint_field(perf, 4, 0U)
    if idle != 0U && self.clock_ms is None {
      raise Unsupported("nonzero peer idle timeout requires receive_at")
    }
    self.peer_idle_ms = idle
    let announced = uint_field(perf, 2, 4294967295U)
    self.peer_frame_max = if announced.to_uint64() >
      self.limits.max_frame_bytes.to_uint64() {
      self.limits.max_frame_bytes
    } else {
      announced.reinterpret_as_int()
    }
    self.peer_channel_max = match perf.field(3) {
      UShort(n) => n
      _ => 65535
    }
    self.remote_open = true
    self.remote_container = Some(perf.field(0).as_text())
    self.event(RemoteOpen(perf.field(0).as_text()))
    return
  }
  if !self.remote_open {
    raise State("remote open required")
  }
  if perf.code == 0x11 {
    self.receive_begin(frame.channel, perf)
    return
  }
  if perf.code == 0x18 {
    if frame.channel != 0 {
      raise Invalid("close channel", 0)
    }
    if !self.io.closing {
      self.queue(self.frame(0x18, []))
    }
    self.closed = true
    self.release_state(preserve=true)
    self.event(RemoteClosed(perf.field(0)))
    return
  }
  let session = match self.remote_sessions.get(frame.channel) {
    Some(session) => session
    None => raise Invalid("unknown remote session channel", 0)
  }
  self.receive_session(session, perf, frame.payload)
}

///|
fn Endpoint::receive_session(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
  payload : Bytes,
) -> Unit raise AmqpError {
  if session.end_sent && perf.code != 0x17 {
    // Frames already in flight before the peer processes End are discarded.
    return
  }
  let remote_handle = match perf.code {
    0x13 =>
      match perf.field(4) {
        UInt(handle) => Some(handle)
        _ => None
      }
    0x14 | 0x16 => Some(uint_field(perf, 0, 0U))
    _ => None
  }
  if remote_handle is Some(handle) {
    match
      session.links
      .iter()
      .find_first(link => link.remote_handle == Some(handle)) {
      Some(link) if link.failed && perf.code != 0x16 => {
        self.end_with_error(
          session, "amqp:session:errant-link", "frame on a failed link",
        )
        return
      }
      None => {
        self.end_with_error(
          session, "amqp:session:unattached-handle", "unknown remote link handle",
        )
        return
      }
      _ => ()
    }
  }
  if perf.code == 0x14 && !session.window.can_receive() {
    self.end_with_error(
      session, "amqp:session:window-violation", "transfer exceeded session receive window",
    )
    return
  }
  match perf.code {
    0x17 => {
      if !session.end_sent {
        self.queue(self.frame(0x17, [], channel=session.channel))
      }
      session.end_sent = true
      self.remote_sessions.remove(session.remote_channel.unwrap()) |> ignore
      self.sessions.remove(session.channel) |> ignore
      self.release_session(session, preserve=true)
      self.session_event(session, SessionEnded(perf.field(0)))
    }
    0x12 => self.receive_attach(session, perf)
    0x13 => self.receive_flow(session, perf)
    0x14 => self.receive_transfer(session, perf, payload)
    0x15 => self.receive_disposition(session, perf)
    0x16 => self.receive_detach(session, perf)
    _ => raise Unsupported("session performative not yet handled")
  }
}

///|
pub fn Endpoint::finish_input(self : Endpoint) -> Unit raise AmqpError {
  errdefer self.transport_lost()
  self.stream.finish()
  if !self.closed {
    raise State("transport EOF without protocol close")
  }
}

///|
test "connection opens and maps distinct local and remote session channels" {
  let a = Endpoint::new("a", channel=3)
  let b = Endpoint::new("b", channel=17)
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(a.primary.remote_channel, Some(17))
  assert_eq(b.primary.remote_channel, Some(3))
  assert_eq(a.events().length(), 2)
}

///|
fn Endpoint::fail_connection(self : Endpoint) -> Unit {
  self.failed = true
  self.release_state()
  self.io.events.clear()
  self.io.event_bytes = 0
  self.io.outbound.clear()
  self.io.queued_bytes = 0
}
