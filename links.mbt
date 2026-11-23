///|
pub fn Endpoint::attach(
  self : Endpoint,
  name : String,
  sender : Bool,
  address : String,
  handle? : UInt32,
  receiver_mode~ : ReceiverSettleMode = First,
  recoverable~ : Bool = false,
  channel~ : Int32 = self.channel,
) -> UInt32 raise AmqpError {
  let session = self.session_active(channel)
  let handle = match handle {
    Some(handle) => handle
    None => session.next_handle()
  }
  if session.links.length() >= self.limits.max_links ||
    handle > session.peer_handle_max {
    raise Limit("link handle count or peer handle-max")
  }
  if text_bytes(name, maximum=4096).is_empty() {
    raise Invalid("link name", 0)
  }
  for link in session.links {
    if (link.name == name && link.sender == sender) ||
      link.reserves_handle(handle) {
      raise State("duplicate local link name or handle")
    }
  }
  let link = link_state(name, handle, sender, address)
  link.receiver_mode = receiver_mode
  link.recoverable = recoverable
  let frame = self.frame(
    0x12,
    link.attach_fields(self.limits),
    channel=session.channel,
  )
  self.queue(frame)
  session.links.push(link)
  handle
}

///|
fn Endpoint::receive_attach(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
) -> Unit raise AmqpError {
  let name = perf.field(0).as_text()
  let remote_handle = uint_field(perf, 1, 0U)
  ignore(text_bytes(name, maximum=4096))
  for existing in session.links {
    if !existing.closed && existing.remote_handle == Some(remote_handle) {
      self.end_with_error(
        session, "amqp:session:handle-in-use", "duplicate remote handle",
      )
      return
    }
  }
  let sender = bool_field(perf, 2, false)
  if perf.field(7) is Null &&
    session.links.any(link => {
      link.failed && link.name == name && link.sender == sender
    }) {
    self.end_with_error(
      session, "amqp:session:errant-link", "reattach of failed link requires unsettled state",
    )
    return
  }
  let key = self.link_key(name, sender)
  let found = session.links
    .iter()
    .find_first(link => {
      link.name == name &&
      link.sender == sender &&
      !link.closed &&
      (link.suspended || (!link.ready && !link.pause_pending))
    })
  if found is Some(link) {
    let reply = if link.suspended {
      Some(self.prepare_reattach(session, link, perf))
    } else {
      if link.resume_pending {
        session.clear_recovery_ids(link)
      }
      link.attach(perf, responding=true)
      None
    }
    self.install_link(session, link, reply)
    return
  }
  if session.links.length() >= self.limits.max_links {
    raise Limit("remote link count")
  }
  let handle = session.resume_handle(None)
  let terminus_spec = perf.field(if sender { 5 } else { 6 })
  let address = try {
    if terminus_spec is Described(ULong(0x30UL), _) ||
      terminus_spec is Described(Symbol("amqp:coordinator:list"), _) {
      raise Unsupported("transaction coordinator")
    }
    terminus_address(terminus_spec, sender).unwrap_or("")
  } catch {
    Unsupported(reason) => {
      self.refuse_attach(session, perf, name, handle, sender, reason)
      return
    }
    error => raise error
  }
  let previous = self.link_bindings.get(key)
  let snapshot = if previous is Some(binding) && binding.link.recoverable {
    Some(binding.link.recovery_snapshot(binding.io.limits))
  } else if self.recovery_store is Some(store) && !(perf.field(7) is Null) {
    store.links.get(key).map(saved => saved.recovery_snapshot(self.limits))
  } else {
    None
  }
  let (link, reply) = if snapshot is Some(saved) {
    let already_retained = match previous {
      Some(binding) if physical_equal(binding.io, self.io) =>
        binding.link.retained_bytes()
      _ => 0UL
    }
    let extra = saved.retained_bytes() - already_retained
    if extra > self.limits.max_value_bytes.to_uint64() {
      raise Limit("retained recovery payload budget")
    }
    self.check_retained_bytes(extra.to_int())
    (saved, self.prepare_reattach(session, saved, perf))
  } else {
    let link = link_state(name, handle, sender, address)
    link.recoverable = recoverable_terminus(terminus_spec, sender)
    link.attach(perf)
    (
      link,
      self.frame(0x12, link.attach_fields(self.limits), channel=session.channel),
    )
  }
  self.install_link(session, link, Some(reply))
  session.links.push(link)
  if self.recovery_store is Some(store) {
    store.links.remove(key)
  }
}

///|
pub fn Endpoint::detach(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  handle : UInt32,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  self.io.check_drop_capacity(session, link)
  self.queue(self.frame(0x16, [UInt(handle), Boolean(true)], channel~))
  self.io.drop_link(session, link)
  link.closed = true
  self.unbind_link(link)
}

///|
fn Endpoint::receive_detach(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
) -> Unit raise AmqpError {
  let handle = uint_field(perf, 0, 0U)
  let mut selected : LinkState? = None
  for link in session.links {
    if link.remote_handle == Some(handle) {
      selected = Some(link)
    }
  }
  let link = match selected {
    Some(link) => link
    None => raise Invalid("unknown detach handle", 0)
  }
  if !bool_field(perf, 1, false) && perf.field(2) is Null {
    self.receive_suspend(session, link)
    return
  }
  self.io.check_drop_capacity(session, link)
  if !link.closed {
    self.queue(
      self.frame(
        0x16,
        [UInt(link.local_handle), Boolean(true)],
        channel=session.channel,
      ),
    )
  }
  link.closed = true
  self.io.drop_link(session, link)
  self.session_event(session, Detached(link.local_handle))
  self.unbind_link(link)
  session.links.retain(l => l.remote_handle != Some(handle))
}

///|
fn EndpointBuffers::check_drop_capacity(
  self : EndpointBuffers,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if session.drop_event_count(link) >
    self.limits.max_pending - self.events.length() {
    raise Limit("detach event budget")
  }
}

///|
fn SessionState::drop_event_count(
  self : SessionState,
  link : LinkState,
) -> Int32 {
  let mut count = 1
  for (_, handle) in self.incoming {
    if handle == link.local_handle {
      count += 1
    }
  }
  for (_, handle) in self.outgoing {
    if handle == link.local_handle {
      count += 1
    }
  }
  count
}

///|
fn EndpointBuffers::drop_link(
  self : EndpointBuffers,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  match link.partial {
    Some(p) => self.partial_bytes -= p.bytes.length()
    None => ()
  }
  link.partial = None
  let incoming = session.incoming
    .keys()
    .to_array()
    .filter(id => session.incoming[id] == link.local_handle)
  let outgoing = session.outgoing
    .keys()
    .to_array()
    .filter(id => session.outgoing[id] == link.local_handle)
  for id in incoming {
    session.forget_incoming(id)
    self.session_event(session, UnsettledLost(id, false))
  }
  for id in outgoing {
    session.forget_outgoing(id)
    self.session_event(session, UnsettledLost(id, true))
  }
  link.records.clear()
  link.peer_records.clear()
}

///|
pub fn Endpoint::link_credit(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  handle : UInt32,
) -> UInt32 raise AmqpError {
  let session = self.session_active(channel)
  session.local_link(handle).credit
}

///|
fn SessionState::next_handle(self : SessionState) -> UInt32 {
  let occupied : Set[UInt32] = Set([])
  for link in self.links {
    if !link.suspended ||
      link.records.values().any(record => record.id is Some(_)) {
      occupied.add(link.local_handle)
    }
  }
  let mut handle = 0U
  while occupied.contains(handle) {
    handle += 1U
  }
  handle
}

///|
fn Endpoint::refuse_attach(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
  name : String,
  handle : UInt32,
  sender : Bool,
  reason : String,
) -> Unit raise AmqpError {
  let link = link_state(name, handle, sender, "")
  link.remote_handle = Some(uint_field(perf, 1, 0U))
  let fields = link.attach_fields(self.limits)
  if sender {
    fields[5] = Null
    fields[6] = perf.field(6)
  } else {
    fields[5] = perf.field(5)
    fields[6] = Null
  }
  let decline = self.frame(0x12, fields, channel=session.channel)
  let error = Described(
    ULong(0x1dUL),
    List([Symbol("amqp:not-implemented"), Text(reason)]),
  )
  let detach = self.frame(
    0x16,
    [UInt(handle), Boolean(true), error],
    channel=session.channel,
  )
  self.queue(decline + detach)
  link.closed = true
  link.failed = true
  session.links.push(link)
}
