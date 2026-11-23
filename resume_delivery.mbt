///|
/// Peer-visible deliveries that can be resumed in the current Attach round.
pub fn Endpoint::resumable_deliveries(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> Array[Bytes] raise AmqpError {
  let link = self.session_active(channel).local_link(handle)
  if !link.ready || !link.sender {
    raise State("recovery needs an attached sender")
  }
  link.records.keys().to_array().filter(tag => link.peer_records.contains(tag))
}

///|
/// Resume one previously unsettled delivery once the receiver grants credit.
/// The sender retains the complete encoded payload until a terminal outcome.
pub fn Endpoint::resume_delivery(
  self : Endpoint,
  handle : UInt32,
  tag : Bytes,
  channel~ : Int32 = self.channel,
) -> UInt32 raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  if !link.ready || !link.sender || link.credit == 0U {
    raise State("recovery needs an attached sender and link credit")
  }
  guard link.records.get(tag) is Some(record) &&
    link.peer_records.get(tag) is Some(peer) &&
    record.id is None else {
    raise State("delivery has no shared unsettled state in this attach round")
  }
  let local_outcome = terminal_state(record.state)
  let peer_outcome = terminal_state(peer)
  let mut aborted = false
  let mut state = Null
  let mut bytes = b""
  let mut outcome = None
  if peer_outcome is Some(peer_result) {
    let selected = local_outcome.unwrap_or(peer_result)
    outcome = Some(selected)
    state = selected.value()
  } else if local_outcome is Some(_) {
    aborted = true
  } else {
    let position = received_fields(peer)
    let offset = match position {
      Some((section, offset)) =>
        recovery_offset(record.bytes, section, offset, self.limits) catch {
          _ => -1
        }
      None => 0
    }
    if offset < 0 {
      aborted = true
    } else {
      state = peer
      bytes = record.bytes[offset:].to_owned()
    }
  }
  let settled = outcome is Some(actual) &&
    peer_outcome == Some(actual) &&
    link.settle_mode != 0
  let id = session.next_delivery
  if session.outgoing.contains(id) ||
    (
      !settled &&
      !aborted &&
      session.outgoing.length() >= self.limits.max_pending
    ) {
    raise Limit("outgoing resumed deliveries")
  }
  if (settled || aborted) && self.io.events.length() >= self.limits.max_pending {
    raise Limit("recovery event capacity")
  }
  let (frames, total) = self.prepare_transfers(
    session,
    handle,
    id,
    tag,
    bytes,
    settled,
    resuming=true,
    state~,
    aborted~,
  )
  self.commit_transfers(session, link, frames, total)
  link.peer_records.remove(tag) |> ignore
  if aborted {
    link.records.remove(tag) |> ignore
    self.session_event(session, UnsettledLost(id, true))
  } else if settled && outcome is Some(result) {
    link.records.remove(tag) |> ignore
    self.session_event(session, Settled(id, result))
  } else {
    session.outgoing[id] = handle
    session.outgoing_tags[id] = tag
    session.recovering_outgoing[id] = (link, tag)
    record.id = Some(id)
    if outcome is Some(result) {
      record.state = result.value()
      record.bytes = b""
    }
  }
  id
}

///|
fn terminal_state(state : Value) -> Outcome? raise AmqpError {
  if state is Null || received_fields(state) is Some(_) {
    None
  } else {
    Some(Outcome::from_value(state))
  }
}

///|
/// Deliveries the peer omitted from a complete map may be retried by the host
/// as new deliveries. They are never treated as omitted from a partial map.
pub fn Endpoint::unseen_deliveries(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> Array[Bytes] raise AmqpError {
  let link = self.session_active(channel).local_link(handle)
  if !link.ready || !link.sender || link.peer_incomplete || link.sent_incomplete {
    return []
  }
  link.records
  .keys()
  .to_array()
  .filter(tag => {
    link.records[tag].id is None && !link.peer_records.contains(tag)
  })
}

///|
/// Release a source-only recovery record and return its message for a new send.
pub fn Endpoint::take_unseen(
  self : Endpoint,
  handle : UInt32,
  tag : Bytes,
  channel~ : Int32 = self.channel,
) -> Message? raise AmqpError {
  let link = self.session_active(channel).local_link(handle)
  if !link.ready ||
    !link.sender ||
    link.peer_incomplete ||
    link.sent_incomplete ||
    link.peer_records.contains(tag) {
    raise State("only source-only records from complete maps can be released")
  }
  guard link.records.get(tag) is Some(record) && record.id is None else {
    raise State("source-only delivery not found")
  }
  let message = if record.bytes.is_empty() {
    None
  } else {
    Some(decode_message(record.bytes, limits=self.limits))
  }
  link.records.remove(tag) |> ignore
  message
}

///|
/// Snapshot of the current Attach round, including the gate for new deliveries.
#derive(Debug, Eq)
pub(all) struct RecoveryStatus {
  local_incomplete : Bool
  peer_incomplete : Bool
  retained : Int32
}

///|
pub extend RecoveryStatus with @debug.Debug::{to_repr}

///|
pub extend RecoveryStatus with Eq::{equal, not_equal}

///|
/// Inspect recovery after LinkReady and decide whether to detach and reattach.
pub fn Endpoint::recovery_status(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> RecoveryStatus raise AmqpError {
  let link = self.session_active(channel).local_link(handle)
  if !link.ready {
    raise State("recovery status needs an attached link")
  }
  {
    local_incomplete: link.sent_incomplete,
    peer_incomplete: link.peer_incomplete,
    retained: link.records.length(),
  }
}
