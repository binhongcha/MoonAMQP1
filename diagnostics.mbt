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
  let outgoing : Map[UInt32, Int32] = Map([])
  let incoming : Map[UInt32, Int32] = Map([])
  for h in self.outgoing.values() {
    outgoing[h] = outgoing.get(h).unwrap_or(0) + 1
  }
  for h in self.incoming.values() {
    incoming[h] = incoming.get(h).unwrap_or(0) + 1
  }
  {
    closed: self.closed,
    failed: self.failed,
    session_ready: self.remote_channel is Some(_) &&
    !self.end_sent &&
    !self.closing &&
    !self.closed &&
    !self.failed,
    queued_bytes: self.queued_bytes,
    partial_bytes: self.partial_bytes,
    queued_events: self.events.length(),
    links: self.links.map(l => {
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
    }),
  }
}
