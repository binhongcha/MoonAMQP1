///|
pub(all) struct LinkDiagnostic {
  name : String
  address : String
  handle : UInt32
  remote_handle : UInt32?
  sender : Bool
  ready : Bool
  closed : Bool
  credit : UInt32
  outgoing_unsettled : Int32
  incoming_unsettled : Int32
  receiving_fragment : Bool
}

///|
pub(all) struct EndpointDiagnostic {
  closed : Bool
  failed : Bool
  session_ready : Bool
  queued_bytes : Int32
  partial_bytes : Int32
  queued_events : Int32
  links : Array[LinkDiagnostic]
}

///|
/// Detached snapshot. Does not consume credit, queued bytes or events.
pub fn Endpoint::diagnostics(self : Endpoint) -> EndpointDiagnostic {
  let session = self.primary
  {
    closed: self.closed,
    failed: self.failed,
    session_ready: session.remote_channel is Some(_) &&
    !session.end_sent &&
    !self.closing &&
    !self.closed &&
    !self.failed,
    queued_bytes: self.queued_bytes,
    partial_bytes: self.partial_bytes,
    queued_events: self.events.length(),
    links: session.link_diagnostics(),
  }
}

///|
fn SessionState::link_diagnostics(self : SessionState) -> Array[LinkDiagnostic] {
  let outgoing : Map[UInt32, Int32] = Map([])
  let incoming : Map[UInt32, Int32] = Map([])
  for h in self.outgoing.values() {
    outgoing[h] = outgoing.get(h).unwrap_or(0) + 1
  }
  for h in self.incoming.values() {
    incoming[h] = incoming.get(h).unwrap_or(0) + 1
  }
  self.links.map(l => {
    name: l.name,
    address: l.address,
    handle: l.local_handle,
    remote_handle: l.remote_handle,
    sender: l.sender,
    ready: l.ready,
    closed: l.closed,
    credit: l.credit,
    outgoing_unsettled: outgoing.get(l.local_handle).unwrap_or(0),
    incoming_unsettled: incoming.get(l.local_handle).unwrap_or(0),
    receiving_fragment: l.partial is Some(_),
  })
}

///|
/// Window and settlement pressure for one session, detached from mutable state.
pub(all) struct SessionDiagnostic {
  channel : Int32
  remote_channel : Int32?
  ending : Bool
  next_incoming : UInt32
  next_outgoing : UInt32
  incoming_window : UInt32
  outgoing_window : UInt32
  peer_incoming_window : UInt32
  outgoing_unsettled : Int32
  incoming_unsettled : Int32
  awaiting_sender_confirmation : Int32
  links : Array[LinkDiagnostic]
}

///|
pub fn Endpoint::session_diagnostics(
  self : Endpoint,
) -> Array[SessionDiagnostic] {
  let sessions = self.sessions.values().to_array()
  sessions.sort_by((a, b) => a.channel.compare(b.channel))
  sessions.map(session => {
    let (next_incoming, next_outgoing) = session.window.positions()
    let (incoming_window, outgoing_window, peer_incoming_window) = session.window.remaining()
    {
      channel: session.channel,
      remote_channel: session.remote_channel,
      ending: session.end_sent,
      next_incoming,
      next_outgoing,
      incoming_window,
      outgoing_window,
      peer_incoming_window,
      outgoing_unsettled: session.outgoing.length(),
      incoming_unsettled: session.incoming.length(),
      awaiting_sender_confirmation: session.reported.length(),
      links: session.link_diagnostics(),
    }
  })
}
