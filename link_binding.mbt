///|
priv struct EndpointBuffers {
  limits : Limits
  channel : Int32
  outbound : Array[Bytes]
  events : Array[Event]
  mut queued_bytes : Int32
  mut partial_bytes : Int32
  mut event_bytes : Int32
  mut closing : Bool
}

///|
fn EndpointBuffers::check_queue(
  self : EndpointBuffers,
  bytes : Int32,
  count : Int32,
) -> Unit raise AmqpError {
  if bytes > self.limits.max_value_bytes - self.queued_bytes ||
    count > self.limits.max_items - self.outbound.length() {
    raise Limit("outbound queue; drain before more commands")
  }
}

///|
/// The caller reserves queue space before changing protocol state.
fn EndpointBuffers::append(self : EndpointBuffers, bytes : Bytes) -> Unit {
  self.outbound.push(bytes)
  self.queued_bytes += bytes.length()
}

///|
fn EndpointBuffers::session_event(
  self : EndpointBuffers,
  session : SessionState,
  event : Event,
) -> Unit raise AmqpError {
  if self.events.length() >= self.limits.max_pending {
    raise Limit("event queue; drain events")
  }
  self.events.push(
    if session.channel == self.channel {
      event
    } else {
      SessionEvent(session.channel, event)
    },
  )
}

///|
// The registry holds session state and buffers without keeping an Endpoint
// alive through the store. Close and transport_lost release its bindings.
priv struct LinkBinding {
  io : EndpointBuffers
  session : SessionState
  link : LinkState
  stolen : Bytes
}

///|
fn Endpoint::link_key(
  self : Endpoint,
  name : String,
  sender : Bool,
) -> (String, String, String, Bool) {
  (self.container, self.remote_container.unwrap_or(""), name, sender)
}

///|
fn Endpoint::unbind_link(self : Endpoint, link : LinkState) -> Unit {
  let key = self.link_key(link.name, link.sender)
  if self.link_bindings.get(key) is Some(binding) &&
    physical_equal(binding.link, link) {
    self.link_bindings.remove(key)
  }
}

///|
fn LinkState::recovery_snapshot(self : LinkState, limits : Limits) -> LinkState {
  let saved = link_state(self.name, 0U, self.sender, self.address)
  saved.recoverable = self.recoverable
  saved.suspended = true
  saved.delivery_count = self.delivery_count
  saved.receiver_mode = self.receiver_mode
  saved.settle_mode = self.settle_mode
  saved.recovery_cursor = self.recovery_cursor
  for (tag, entry) in self.records {
    saved.records[tag] = {
      id: None,
      state: entry.state,
      bytes: entry.bytes,
      delivered: entry.delivered,
    }
  }
  saved.partial = self.partial
  saved.save_partial(limits)
  saved.partial = None
  saved
}

///|
fn Endpoint::install_link(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  reply : Bytes?,
) -> Unit raise AmqpError {
  let key = self.link_key(link.name, link.sender)
  let binding = self.link_bindings.get(key)
  let previous = if binding is Some(prior) && !physical_equal(prior.link, link) {
    Some(prior)
  } else {
    None
  }
  let stolen = Described(ULong(0x1dUL), List([Symbol("amqp:link:stolen")]))
  let detach = self.frame(
    0x16,
    [UInt(link.local_handle), Boolean(true), stolen],
    channel=session.channel,
  )
  let mut outstanding_events = 1
  if !link.sender && !link.peer_incomplete {
    for tag in link.records.keys() {
      if !link.peer_records.contains(tag) {
        outstanding_events += 1
      }
    }
  }
  let mut queue_bytes = reply.map(bytes => bytes.length()).unwrap_or(0)
  let mut queue_count = if reply is Some(_) { 1 } else { 0 }
  let mut send_stolen = false
  if previous is Some(prior) {
    send_stolen = !prior.link.suspended &&
      !prior.session.end_sent &&
      !prior.io.closing
    let drop_events = prior.session.drop_event_count(prior.link)
    if physical_equal(prior.io, self.io) {
      outstanding_events = checked_add(outstanding_events, drop_events)
      if send_stolen {
        queue_bytes = checked_add(queue_bytes, prior.stolen.length())
        queue_count += 1
      }
    } else {
      prior.io.check_drop_capacity(prior.session, prior.link)
      if send_stolen {
        prior.io.check_queue(prior.stolen.length(), 1)
      }
    }
  }
  if outstanding_events > self.limits.max_pending - self.io.events.length() {
    raise Limit("link activation event budget")
  }
  self.io.check_queue(queue_bytes, queue_count)

  if previous is Some(prior) {
    prior.evict(send_stolen)
  }
  if reply is Some(reply) {
    self.io.append(reply)
  }
  self.link_bindings[key] = { io: self.io, session, link, stolen: detach, }
  self.reconcile_recovery(session, link)
  self.session_event(
    session,
    LinkReady(link.local_handle, link.name, link.sender),
  )
}

///|
fn LinkBinding::evict(
  self : LinkBinding,
  send_stolen : Bool,
) -> Unit raise AmqpError {
  let link = self.link
  let session = self.session
  if send_stolen {
    self.io.append(self.stolen)
    let pending = if link.sender { session.outgoing } else { session.incoming }
    link.failed_deliveries = pending
      .keys()
      .to_array()
      .filter(id => pending[id] == link.local_handle)
    link.failed = true
  }
  self.io.drop_link(session, link)
  link.closed = true
  link.ready = false
  self.io.session_event(session, Detached(link.local_handle))
  if !send_stolen && link.remote_handle is None {
    session.links.retain(other => !physical_equal(other, link))
  }
}
