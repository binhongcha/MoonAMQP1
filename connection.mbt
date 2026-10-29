///|
pub fn Endpoint::begin(self : Endpoint) -> Unit raise AmqpError {
  self.active()
  if !self.remote_open || self.begin_sent {
    raise State("open must precede one session begin")
  }
  if self.channel > self.peer_channel_max {
    raise State("local channel exceeds peer channel-max")
  }
  let (_, next_out) = self.window.positions()
  let (incoming, outgoing, _) = self.window.remaining()
  self.queue(
    self.frame(0x11, [Null, UInt(next_out), UInt(incoming), UInt(outgoing)]),
  )
  self.begin_sent = true
}

///|
pub fn Endpoint::receive(
  self : Endpoint,
  bytes : Bytes,
) -> Unit raise AmqpError {
  self.active()
  errdefer {
    self.failed = true
    self.outbound.clear()
    self.queued_bytes = 0
  }
  let items = self.stream.feed(bytes)
  for item in items {
    match item {
      Header => if !self.started { self.start() }
      AmqpFrame(frame) => self.receive_frame(frame)
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
    None => return
    Some(perf) => perf
  }
  if perf.code == 0x10 {
    if frame.channel != 0 || self.remote_open {
      raise Invalid("duplicate or nonzero-channel open", 0)
    }
    if !(perf.field(4) is Null) && uint_field(perf, 4, 0U) != 0U {
      raise Unsupported(
        "idle timeout requires host timer integration; request zero",
      )
    }
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
    self.event(RemoteOpen(perf.field(0).as_text()))
    return
  }
  if !self.remote_open {
    raise State("remote open required")
  }
  if perf.code == 0x11 {
    if self.remote_channel is Some(_) {
      raise Unsupported("multiple sessions")
    }
    let responding = match perf.field(0) {
      Null => false
      UShort(channel) if channel == self.channel && self.begin_sent => true
      _ => raise Invalid("remote-channel does not map local session", 0)
    }
    if !responding && self.begin_sent {
      raise Unsupported("crossed begin requires multiple-session host")
    }
    self.window.establish(
      uint_field(perf, 1, 0U),
      uint_field(perf, 2, 0U),
      uint_field(perf, 3, 0U),
    )
    self.remote_channel = Some(frame.channel)
    self.peer_handle_max = uint_field(perf, 4, 4294967295U)
    if !responding {
      let (incoming, outgoing, _) = self.window.remaining()
      self.queue(
        self.frame(0x11, [
          UShort(frame.channel),
          UInt(self.window.next_out),
          UInt(incoming),
          UInt(outgoing),
        ]),
      )
      self.begin_sent = true
    }
    self.event(SessionReady)
    return
  }
  if perf.code == 0x18 {
    if frame.channel != 0 {
      raise Invalid("close channel", 0)
    }
    self.queue(self.frame(0x18, []))
    self.closed = true
    self.event(RemoteClosed(perf.field(0)))
    return
  }
  if self.remote_channel != Some(frame.channel) {
    raise Invalid("unknown remote session channel", 0)
  }
  self.receive_session(perf, frame.payload)
}

///|
fn Endpoint::receive_session(
  self : Endpoint,
  perf : Performative,
  _payload : Bytes,
) -> Unit raise AmqpError {
  match perf.code {
    0x17 => {
      self.queue(self.frame(0x17, []))
      self.remote_channel = None
      self.closed = true
      self.event(RemoteClosed(perf.field(0)))
    }
    0x12 => self.receive_attach(perf)
    0x13 => self.receive_flow(perf)
    0x16 => self.receive_detach(perf)
    _ => raise Unsupported("session performative not yet handled")
  }
}

///|
pub fn Endpoint::finish_input(self : Endpoint) -> Unit raise AmqpError {
  errdefer {
    self.failed = true
  }
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
  assert_eq(a.remote_channel, Some(17))
  assert_eq(b.remote_channel, Some(3))
  assert_eq(a.events().length(), 2)
}
