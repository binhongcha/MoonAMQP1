///|
pub fn Endpoint::acknowledge(
  self : Endpoint,
  id : UInt32,
  outcome : Outcome,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  self.acknowledge_range(id, id, outcome, channel~)
}

///|
/// Report the same outcome for a contiguous range of complete incoming deliveries.
pub fn Endpoint::acknowledge_range(
  self : Endpoint,
  first : UInt32,
  last : UInt32,
  outcome : Outcome,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let span = serial_distance(first, last)
  if span.to_uint64() >= session.incoming.length().to_uint64() {
    raise State("acknowledgement range contains unknown delivery")
  }
  let settle = session.incoming_first.contains(first)
  for offset in 0U..<=span {
    let id = first + offset
    if !session.incoming.contains(id) || session.reported.contains(id) {
      raise State("acknowledgement requires known unreported deliveries")
    }
    if session.incoming_first.contains(id) != settle {
      raise State("acknowledgement range mixes receiver settlement modes")
    }
  }
  for link in session.links {
    if link.partial is Some(partial) && partial.id - first <= span {
      raise State("cannot acknowledge an incomplete delivery")
    }
  }
  self.queue(
    self.disposition_frame(session, true, first, last, settle, outcome.value()),
  )
  for offset in 0U..<=span {
    let id = first + offset
    if settle {
      session.forget_incoming(id)
    } else {
      session.reported[id] = outcome
    }
  }
}

///|
fn SessionState::forget_incoming(self : SessionState, id : UInt32) -> Unit {
  self.incoming.remove(id) |> ignore
  self.incoming_tags.remove(id) |> ignore
  self.incoming_first.remove(id)
  self.reported.remove(id) |> ignore
}

///|
fn Endpoint::disposition_frame(
  self : Endpoint,
  session : SessionState,
  receiver : Bool,
  first : UInt32,
  last : UInt32,
  settled : Bool,
  state : Value,
) -> Bytes raise AmqpError {
  let fields = [
    Boolean(receiver),
    UInt(first),
    if last == first {
      Null
    } else {
      UInt(last)
    },
    Boolean(settled),
  ]
  if !(state is Null) {
    fields.push(state)
  }
  self.frame(0x15, fields, channel=session.channel)
}

///|
fn Endpoint::receive_disposition(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
) -> Unit raise AmqpError {
  let first = uint_field(perf, 1, 0U)
  let last = uint_field(perf, 2, first)
  let span = serial_distance(first, last)
  let settled = bool_field(perf, 3, false)
  let remote_receiver = bool_field(perf, 0, false)
  let pending = if remote_receiver {
    session.outgoing
  } else {
    session.incoming
  }
  let ids : Array[UInt32] = []
  for id in pending.keys() {
    if id - first <= span {
      ids.push(id)
    }
  }
  if ids.is_empty() {
    return
  }
  if ids.length() > self.limits.max_pending - self.events.length() {
    raise Limit("settlement event capacity")
  }
  ids.sort_by((a, b) => (a - first).compare(b - first))
  let state = perf.field(4)
  if remote_receiver {
    let outcome = Outcome::from_value(state)
    if !settled {
      let second_handles : Set[UInt32] = Set([])
      for link in session.links {
        if link.receiver_mode == Second {
          second_handles.add(link.local_handle)
        }
      }
      for id in ids {
        if !second_handles.contains(pending[id]) {
          raise Invalid("unsettled outcome on receiver-first link", 0)
        }
      }
      // Only confirm deliveries tracked here. Coalesce contiguous known IDs.
      let mut start = ids[0]
      let mut end = start
      for i in 1..<ids.length() {
        let id = ids[i]
        if id != end + 1U {
          self.queue(
            self.disposition_frame(session, false, start, end, true, Null),
          )
          start = id
        }
        end = id
      }
      self.queue(self.disposition_frame(session, false, start, end, true, Null))
    }
    for id in ids {
      session.outgoing.remove(id) |> ignore
      session.outgoing_tags.remove(id) |> ignore
      self.session_event(session, Settled(id, outcome))
    }
  } else {
    if !settled {
      raise Unsupported("unsettled sender disposition")
    }
    for link in session.links {
      if link.partial is Some(partial) && partial.id - first <= span {
        raise Unsupported("settlement of incomplete delivery")
      }
    }
    let outcome = if state is Null {
      None
    } else {
      Some(Outcome::from_value(state))
    }
    for id in ids {
      if outcome is Some(actual) && session.reported.get(id) != Some(actual) {
        raise Unsupported("sender disposition with a new outcome")
      }
      session.forget_incoming(id)
      self.session_event(session, PeerSettled(id))
    }
  }
}
