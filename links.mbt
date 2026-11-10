///|
pub fn Endpoint::attach(
  self : Endpoint,
  name : String,
  sender : Bool,
  address : String,
  handle? : UInt32,
  receiver_mode~ : ReceiverSettleMode = First,
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
    if link.name == name || link.local_handle == handle {
      raise State("duplicate local link name or handle")
    }
  }
  let link = link_state(name, handle, sender, address)
  link.receiver_mode = receiver_mode
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
      raise Invalid("duplicate remote handle", 0)
    }
  }
  let mut found : LinkState? = None
  for link in session.links {
    if link.name == name && !link.closed {
      found = Some(link)
    }
  }
  let link = match found {
    Some(link) => {
      link.attach(perf, responding=true)
      link
    }
    None => {
      if session.links.length() >= self.limits.max_links {
        raise Limit("remote link count")
      }
      let handle = session.next_handle()
      if handle > session.peer_handle_max {
        raise Limit("peer handle-max")
      }
      let sender = bool_field(perf, 2, false)
      let address = terminus_address(
        perf.field(if sender { 5 } else { 6 }),
        sender,
      ).unwrap_or("")
      let link = link_state(name, handle, sender, address)
      link.attach(perf)
      self.queue(
        self.frame(
          0x12,
          link.attach_fields(self.limits),
          channel=session.channel,
        ),
      )
      session.links.push(link)
      link
    }
  }
  self.session_event(
    session,
    LinkReady(link.local_handle, link.name, link.sender),
  )
}

///|
pub fn Endpoint::detach(
  self : Endpoint,
  channel~ : Int32 = self.channel,
  handle : UInt32,
) -> Unit raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  self.check_drop_capacity(session, link)
  self.queue(self.frame(0x16, [UInt(handle), Boolean(true)], channel~))
  self.drop_link(session, link)
  link.closed = true
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
  if !bool_field(perf, 1, false) {
    raise Unsupported("link detach without close and resume")
  }
  self.check_drop_capacity(session, link)
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
  self.drop_link(session, link)
  self.session_event(session, Detached(link.local_handle))
}

///|
fn Endpoint::check_drop_capacity(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  let mut count = 1
  for (_, handle) in session.incoming {
    if handle == link.local_handle {
      count += 1
    }
  }
  for (_, handle) in session.outgoing {
    if handle == link.local_handle {
      count += 1
    }
  }
  if count > self.limits.max_pending - self.events.length() {
    raise Limit("detach event budget")
  }
}

///|
fn Endpoint::drop_link(
  self : Endpoint,
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
    session.outgoing.remove(id) |> ignore
    session.outgoing_tags.remove(id) |> ignore
    self.session_event(session, UnsettledLost(id, true))
  }
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
    occupied.add(link.local_handle)
  }
  let mut handle = 0U
  while occupied.contains(handle) {
    handle += 1U
  }
  handle
}
