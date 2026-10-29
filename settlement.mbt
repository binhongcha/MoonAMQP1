///|
pub fn Endpoint::acknowledge(
  self : Endpoint,
  id : UInt32,
  outcome : Outcome,
) -> Unit raise AmqpError {
  self.session_active()
  if !self.incoming.contains(id) {
    raise State("no unsettled incoming delivery with this ID")
  }
  let frame = self.frame(0x15, [
    Boolean(true),
    UInt(id),
    Null,
    Boolean(true),
    outcome.value(),
  ])
  self.queue(frame)
  self.incoming.remove(id) |> ignore
}

///|
fn Endpoint::receive_disposition(
  self : Endpoint,
  perf : Performative,
) -> Unit raise AmqpError {
  let first = uint_field(perf, 1, 0U)
  let last = uint_field(perf, 2, first)
  let span = serial_distance(first, last)
  if !bool_field(perf, 3, false) {
    raise Unsupported(
      "unsettled disposition; receiver settle mode first required",
    )
  }
  let remote_receiver = bool_field(perf, 0, false)
  let pending = if remote_receiver { self.outgoing } else { self.incoming }
  let ids : Array[UInt32] = []
  for id in pending.keys() {
    if id - first <= span {
      ids.push(id)
    }
  }
  ids.sort_by((a, b) => (a - first).compare(b - first))
  let outcome = if remote_receiver {
    Some(Outcome::from_value(perf.field(4)))
  } else {
    if !(perf.field(4) is Null) {
      raise Unsupported("sender disposition with outcome")
    }
    None
  }
  if ids.length() > self.limits.max_pending - self.events.length() {
    raise Limit("settlement event capacity")
  }
  for id in ids {
    pending.remove(id) |> ignore
    match outcome {
      Some(outcome) => self.event(Settled(id, outcome))
      None => self.event(PeerSettled(id))
    }
  }
}
