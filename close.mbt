///|
pub fn Endpoint::close(self : Endpoint) -> Unit raise AmqpError {
  self.active()
  if !self.remote_open || self.closing {
    raise State("cannot close before remote open or close twice")
  }
  self.queue(self.frame(0x18, []))
  self.closing = true
}

///|
pub fn Endpoint::end_session(self : Endpoint) -> Unit raise AmqpError {
  self.session_active()
  self.queue(self.frame(0x17, []))
  self.end_sent = true
}

///|
pub fn Endpoint::heartbeat(self : Endpoint) -> Unit raise AmqpError {
  self.active()
  if !self.remote_open || self.closing {
    raise State("heartbeat requires open connection")
  }
  self.queue(
    encode_frame(
      { channel: 0, performative: None, payload: b"", extended_header: b"", },
      limits=self.limits,
    ),
  )
}

///|
fn Endpoint::release_state(self : Endpoint) -> Unit {
  for link in self.links {
    link.closed = true
    link.partial = None
  }
  self.incoming.clear()
  self.outgoing.clear()
}

///|
pub fn Endpoint::session_positions(self : Endpoint) -> (UInt32, UInt32) {
  self.window.positions()
}
