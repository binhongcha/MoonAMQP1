///|
fn SessionState::check_quiescent(
  self : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if link.partial is Some(_) ||
    link.draining ||
    self.incoming.values().any(h => h == link.local_handle) ||
    self.outgoing.values().any(h => h == link.local_handle) {
    raise Unsupported(
      "suspension requires no unsettled, partial or draining delivery",
    )
  }
}

///|
/// Detach while preserving the link and any opted-in unsettled state.
pub fn Endpoint::suspend_link(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  if !link.ready || (!link.recoverable && !link.sender && link.credit != 0U) {
    raise State("receiver must exhaust or drain credit before suspension")
  }
  if !link.recoverable {
    session.check_quiescent(link)
  }
  self.queue(self.frame(0x16, [UInt(handle), Boolean(false)], channel~))
  link.pause_pending = true
  link.ready = false
}

///|
fn Endpoint::receive_suspend(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if link.closed {
    raise State("cannot suspend a closed link")
  }
  if !link.recoverable {
    session.check_quiescent(link)
  }
  if !link.pause_pending {
    self.queue(
      self.frame(
        0x16,
        [UInt(link.local_handle), Boolean(false)],
        channel=session.channel,
      ),
    )
  }
  link.save_partial(self.limits)
  if link.partial is Some(partial) {
    self.io.partial_bytes -= partial.bytes.length()
    link.partial = None
  }
  link.ready = false
  link.pause_pending = false
  link.suspended = true
  link.credit = 0U
  link.drain_mode = false
  link.draining = false
  link.drain_returned = 0U
  link.remote_handle = None
  self.session_event(session, LinkSuspended(link.name, link.local_handle))
}

///|
fn SessionState::resume_handle(
  self : SessionState,
  requested : UInt32?,
) -> UInt32 raise AmqpError {
  let handle = requested.unwrap_or_else(() => self.next_handle())
  if handle > self.peer_handle_max {
    raise Limit("resume peer handle-max")
  }
  if self.links.any(l => l.reserves_handle(handle)) {
    raise State("resume handle is already mapped")
  }
  handle
}

///|
/// Reattach a suspended link in the same session or import it from the
/// endpoint recovery store. Obtain fresh credit before sending transfers.
pub fn Endpoint::resume_link(
  self : Endpoint,
  name : String,
  handle? : UInt32,
  channel~ : Int32 = self.channel,
) -> UInt32 raise AmqpError {
  let session = self.session_active(channel)
  let candidates = session.links.filter(l => l.name == name && l.suspended)
  if candidates.length() > 1 {
    raise State("ambiguous suspended link direction")
  }
  if candidates.get(0) is Some(link) {
    return self.start_recovery_attach(session, link, handle)
  }
  if self.recovery_store is Some(store) && self.remote_container is Some(remote) {
    let keys = store.links
      .keys()
      .to_array()
      .filter(key => key.0 == self.container && key.1 == remote && key.2 == name)
    if keys.length() > 1 {
      raise State("ambiguous retained link direction")
    }
    if keys.get(0) is Some(key) {
      if session.links.length() >= self.limits.max_links {
        raise Limit("local recovery link count")
      }
      let link = store.links[key]
      self.check_retained_bytes(link.retained_bytes().to_int())
      let result = self.start_recovery_attach(session, link, handle)
      store.links.remove(key) |> ignore
      session.links.push(link)
      return result
    }
  }
  raise State("unknown suspended link")
}

///|
fn Endpoint::prepare_reattach(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  perf : Performative,
) -> Bytes raise AmqpError {
  let handle = session.resume_handle(None)
  session.clear_recovery_ids(link)
  link.attach(perf, responding=true)
  let (frame, incomplete, cursor) = self.attach_recovery_frame(
    session, link, handle,
  )
  link.local_handle = handle
  link.suspended = false
  link.sent_incomplete = incomplete
  link.recovery_cursor = cursor
  frame
}

///|
fn LinkState::reserves_handle(self : LinkState, handle : UInt32) -> Bool {
  self.local_handle == handle &&
  (!self.suspended || self.records.values().any(record => record.id is Some(_)))
}

///|
fn Endpoint::start_recovery_attach(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  requested : UInt32?,
) -> UInt32 raise AmqpError {
  let handle = session.resume_handle(requested)
  let (frame, incomplete, cursor) = self.attach_recovery_frame(
    session, link, handle,
  )
  self.queue(frame)
  link.local_handle = handle
  link.suspended = false
  link.resume_pending = true
  link.sent_incomplete = incomplete
  link.recovery_cursor = cursor
  handle
}
