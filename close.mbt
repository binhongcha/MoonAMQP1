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
pub fn Endpoint::end_session(
  self : Endpoint,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  self.queue(self.frame(0x17, [], channel~))
  session.end_sent = true
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
  for session in self.sessions.values() {
    self.release_session(session)
  }
  self.sessions.clear()
  self.remote_sessions.clear()
}

///|
fn Endpoint::release_session(self : Endpoint, session : SessionState) -> Unit {
  session.remote_channel = None
  for link in session.links {
    link.closed = true
    if link.partial is Some(partial) {
      self.partial_bytes -= partial.bytes.length()
    }
    link.partial = None
  }
  session.incoming.clear()
  session.outgoing.clear()
  session.outgoing_tags.clear()
  session.incoming_tags.clear()
  session.incoming_first.clear()
  session.reported.clear()
}

///|
pub fn Endpoint::session_positions(self : Endpoint) -> (UInt32, UInt32) {
  self.primary.window.positions()
}
