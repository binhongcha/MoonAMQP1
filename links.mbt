///|
pub fn Endpoint::attach(
  self : Endpoint,
  name : String,
  sender : Bool,
  address : String,
  handle~ : UInt32 = self.links.length().reinterpret_as_uint(),
) -> UInt32 raise AmqpError {
  self.session_active()
  if self.links.length() >= self.limits.max_links ||
    handle > self.peer_handle_max {
    raise Limit("link handle count or peer handle-max")
  }
  if text_bytes(name, maximum=4096).is_empty() {
    raise Invalid("link name", 0)
  }
  for link in self.links {
    if link.name == name || link.local_handle == handle {
      raise State("duplicate local link name or handle")
    }
  }
  let link = link_state(name, handle, sender, address)
  let frame = self.frame(0x12, link.attach_fields(self.limits))
  self.queue(frame)
  self.links.push(link)
  handle
}

///|
fn Endpoint::receive_attach(
  self : Endpoint,
  perf : Performative,
) -> Unit raise AmqpError {
  let name = perf.field(0).as_text()
  let remote_handle = uint_field(perf, 1, 0U)
  ignore(text_bytes(name, maximum=4096))
  for existing in self.links {
    if !existing.closed && existing.remote_handle == Some(remote_handle) {
      raise Invalid("duplicate remote handle", 0)
    }
  }
  let mut found : LinkState? = None
  for link in self.links {
    if link.name == name && !link.closed {
      found = Some(link)
    }
  }
  let link = match found {
    Some(link) => {
      link.attach(perf)
      link
    }
    None => {
      if self.links.length() >= self.limits.max_links {
        raise Limit("remote link count")
      }
      let handle = self.links.length().reinterpret_as_uint()
      if handle > self.peer_handle_max {
        raise Limit("peer handle-max")
      }
      let sender = bool_field(perf, 2, false)
      let address = terminus_address(
        perf.field(if sender { 5 } else { 6 }),
        sender,
      ).unwrap_or("")
      let link = link_state(name, handle, sender, address)
      link.attach(perf)
      self.queue(self.frame(0x12, link.attach_fields(self.limits)))
      self.links.push(link)
      link
    }
  }
  self.event(LinkReady(link.local_handle, link.name, link.sender))
}

///|
pub fn Endpoint::detach(
  self : Endpoint,
  handle : UInt32,
) -> Unit raise AmqpError {
  self.session_active()
  let link = self.local_link(handle)
  self.check_drop_capacity(link)
  self.queue(self.frame(0x16, [UInt(handle), Boolean(true)]))
  self.drop_link(link)
  link.closed = true
}

///|
fn Endpoint::receive_detach(
  self : Endpoint,
  perf : Performative,
) -> Unit raise AmqpError {
  let handle = uint_field(perf, 0, 0U)
  let mut selected : LinkState? = None
  for link in self.links {
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
  self.check_drop_capacity(link)
  if !link.closed {
    self.queue(self.frame(0x16, [UInt(link.local_handle), Boolean(true)]))
  }
  link.closed = true
  self.drop_link(link)
  self.event(Detached(link.local_handle))
}

///|
fn Endpoint::check_drop_capacity(
  self : Endpoint,
  link : LinkState,
) -> Unit raise AmqpError {
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
  if count > self.limits.max_pending - self.events.length() {
    raise Limit("detach event budget")
  }
}

///|
fn Endpoint::drop_link(
  self : Endpoint,
  link : LinkState,
) -> Unit raise AmqpError {
  match link.partial {
    Some(p) => self.partial_bytes -= p.bytes.length()
    None => ()
  }
  link.partial = None
  let incoming = self.incoming
    .keys()
    .to_array()
    .filter(id => self.incoming[id] == link.local_handle)
  let outgoing = self.outgoing
    .keys()
    .to_array()
    .filter(id => self.outgoing[id] == link.local_handle)
  for id in incoming {
    self.incoming.remove(id) |> ignore
    self.event(UnsettledLost(id, false))
  }
  for id in outgoing {
    self.outgoing.remove(id) |> ignore
    self.event(UnsettledLost(id, true))
  }
}

///|
pub fn Endpoint::link_credit(
  self : Endpoint,
  handle : UInt32,
) -> UInt32 raise AmqpError {
  self.local_link(handle).credit
}
