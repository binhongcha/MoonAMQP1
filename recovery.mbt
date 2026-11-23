///|
/// In-memory terminus state shared by consecutive connections of one host.
/// Select the store only after the host has authenticated the remote principal.
pub struct RecoveryStore {
  priv links : Map[(String, String, String, Bool), LinkState]
  priv bindings : Map[(String, String, String, Bool), LinkBinding]
  priv max_links : Int32
  priv max_payload_bytes : Int32
}

///|
pub fn RecoveryStore::new(
  max_links~ : Int32 = 256,
  max_payload_bytes~ : Int32 = 16777216,
) -> RecoveryStore raise AmqpError {
  require_range(max_links, 1, 65536)
  require_range(max_payload_bytes, 1, 2147483647)
  { links: Map([]), bindings: Map([]), max_links, max_payload_bytes, }
}

///|
pub fn RecoveryStore::retained_links(self : RecoveryStore) -> Int32 {
  self.links.length()
}

///|
/// Explicitly expire an inactive terminus and release its retained deliveries.
pub fn RecoveryStore::forget(
  self : RecoveryStore,
  local_container : String,
  remote : String,
  name : String,
  sender : Bool,
) -> Bool {
  let key = (local_container, remote, name, sender)
  let found = self.links.contains(key)
  self.links.remove(key)
  found
}

///|
priv struct RecoveryDelivery {
  mut id : UInt32?
  mut state : Value
  mut bytes : Bytes
  mut delivered : Bool
}

///|
fn recoverable_terminus(value : Value, source_side : Bool) -> Bool {
  match value {
    Described(ULong(code), List(fields)) if code ==
      (if source_side { 0x28UL } else { 0x29UL }) =>
      field_at(fields, 2) is Symbol("never")
    Described(Symbol(name), List(fields)) if name ==
      (if source_side { "amqp:source:list" } else { "amqp:target:list" }) =>
      field_at(fields, 2) is Symbol("never")
    _ => false
  }
}

///|
fn persistent_terminus(address : String, source_side : Bool) -> Value {
  Described(
    ULong(if source_side { 0x28UL } else { 0x29UL }),
    List([Text(address), Null, Symbol("never")]),
  )
}

///|
fn received_state(section : UInt32, offset : UInt64) -> Value {
  Described(ULong(0x23UL), List([UInt(section), ULong(offset)]))
}

///|
fn received_fields(state : Value) -> (UInt32, UInt64)? raise AmqpError {
  match state {
    Described(ULong(0x23UL), _) | Described(Symbol("amqp:received:list"), _) => {
      let fields = described_list(state, 0x23UL, "amqp:received:list")
      match fields {
        [UInt(section), ULong(offset)] => Some((section, offset))
        _ => raise Invalid("received state fields", 0)
      }
    }
    _ => None
  }
}

///|
fn decode_unsettled(value : Value) -> Map[Bytes, Value] raise AmqpError {
  let result : Map[Bytes, Value] = Map([])
  match value {
    Null => ()
    MapValue(entries) =>
      for (key, state) in entries {
        guard key is Binary(tag) && tag.length() <= 32 && !result.contains(tag) else {
          raise Invalid("invalid or duplicate unsettled delivery-tag", 0)
        }
        if !(state is Null) && received_fields(state) is None {
          ignore(Outcome::from_value(state))
        }
        result[tag] = state
      }
    _ => raise Invalid("unsettled map", 0)
  }
  result
}

///|
/// Report a prefix as a section number and an offset in its encoded bytes.
fn prefix_state(bytes : Bytes, limits : Limits) -> Value {
  let reader = wire_reader(bytes, limits) catch {
    _ => return received_state(0U, 0UL)
  }
  let mut section = 0U
  while reader.pos < reader.end {
    let start = reader.pos
    ignore(read_value(reader, 0)) catch {
      _ => return received_state(section, (bytes.length() - start).to_uint64())
    }
    section += 1U
  }
  received_state(section, 0UL)
}

///|
fn recovery_offset(
  bytes : Bytes,
  section : UInt32,
  offset : UInt64,
  limits : Limits,
) -> Int32 raise AmqpError {
  let reader = wire_reader(bytes, limits)
  let mut seen = 0U
  while seen < section {
    if reader.pos >= reader.end {
      raise Invalid("received section beyond available payload", 0)
    }
    ignore(read_value(reader, 0))
    seen += 1U
  }
  let start = reader.pos
  let end = if start == reader.end {
    start
  } else {
    try {
      ignore(read_value(reader, 0))
      reader.pos
    } catch {
      _ => reader.end
    }
  }
  if offset > (end - start).to_uint64() {
    raise Invalid("received offset beyond available section bytes", 0)
  }
  start + offset.to_int()
}

///|
fn LinkState::retained_bytes(self : LinkState) -> UInt64 {
  let mut bytes = 0UL
  for record in self.records.values() {
    bytes += record.bytes.length().to_uint64()
  }
  bytes
}

///|
fn Endpoint::check_retained_bytes(
  self : Endpoint,
  extra : Int32,
) -> Unit raise AmqpError {
  let mut bytes = extra.to_uint64()
  for session in self.sessions.values() {
    for link in session.links {
      bytes += link.retained_bytes()
    }
  }
  if bytes > self.limits.max_value_bytes.to_uint64() {
    raise Limit("retained recovery payload budget")
  }
}

///|
fn LinkState::save_partial(self : LinkState, limits : Limits) -> Unit {
  if self.partial is Some(partial) &&
    self.records.get(partial.tag) is Some(record) &&
    (
      record.state is Null ||
      record.state is Described(ULong(0x23UL), _) ||
      record.state is Described(Symbol("amqp:received:list"), _)
    ) {
    record.bytes = Bytes::from_array(partial.bytes)
    record.state = prefix_state(record.bytes, limits)
  }
}

///|
fn Endpoint::save_recovery_link(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
) -> Unit {
  guard link.recoverable &&
    self.recovery_store is Some(store) &&
    self.remote_container is Some(remote) else {
    return
  }
  link.save_partial(self.limits)
  let key = (self.container, remote, link.name, link.sender)
  let mut bytes = link.retained_bytes()
  for (existing, retained) in store.links {
    if existing != key {
      bytes += retained.retained_bytes()
    }
  }
  if bytes > store.max_payload_bytes.to_uint64() ||
    (store.links.length() >= store.max_links && !store.links.contains(key)) {
    // Preserve existing snapshots; report overflow if the event queue has room.
    if self.io.events.length() < self.limits.max_pending {
      self.io.events.push(RecoveryStoreFull(link.name))
    }
    return
  }
  session.clear_recovery_ids(link)
  store.links[key] = link.recovery_snapshot(self.limits)
}

///|
/// Report transport interruption before constructing the next endpoint.
pub fn Endpoint::transport_lost(self : Endpoint) -> Unit {
  if !self.closed && !self.failed {
    self.failed = true
    self.release_state(preserve=true)
    self.io.outbound.clear()
    self.io.queued_bytes = 0
  }
}

///|
fn SessionState::clear_recovery_ids(
  self : SessionState,
  link : LinkState,
) -> Unit {
  for entry in link.records.values() {
    if entry.id is Some(id) {
      if link.sender {
        self.forget_outgoing(id, keep=true)
      } else {
        self.forget_incoming(id, keep=true)
      }
      entry.id = None
    }
  }
}

///|
fn Endpoint::attach_recovery_frame(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  handle : UInt32,
) -> (Bytes, Bool, Int32) raise AmqpError {
  let fields = link.attach_fields(self.limits)
  fields[1] = UInt(handle)
  let entries : Array[(Value, Value)] = []
  for (tag, record) in link.records {
    entries.push((Binary(tag), record.state))
  }
  fields[7] = MapValue(entries)
  try {
    return (self.frame(0x12, fields, channel=session.channel), false, 0)
  } catch {
    Limit(_) => ()
    error => raise error
  }
  fields[8] = Boolean(true)
  // Rotate the offered prefix so an unchanged early record cannot hide all
  // later recoverable deliveries on every reattach.
  let count = entries.length()
  let start = if count == 0 { 0 } else { link.recovery_cursor % count }
  let ordered = entries[start:].to_owned()
  ordered.append(entries[:start])
  let mut low = 0
  let mut high = entries.length()
  while low + 1 < high {
    let middle = low + (high - low) / 2
    fields[7] = MapValue(ordered[:middle].to_owned())
    try {
      ignore(self.frame(0x12, fields, channel=session.channel))
      low = middle
    } catch {
      Limit(_) => high = middle
      error => raise error
    }
  }

  fields[7] = MapValue(ordered[:low].to_owned())
  let advance = if low == 0 { 1 } else { low }
  let cursor = if count == 0 { 0 } else { (start + advance) % count }
  (self.frame(0x12, fields, channel=session.channel), true, cursor)
}

///|
fn Endpoint::reconcile_recovery(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
) -> Unit raise AmqpError {
  if link.sender || link.peer_incomplete {
    return
  }
  let settled = link.records
    .keys()
    .to_array()
    .filter(tag => !link.peer_records.contains(tag))
  if settled.length() > self.limits.max_pending - self.io.events.length() {
    raise Limit("recovery settlement event budget")
  }
  for tag in settled {
    link.records.remove(tag) |> ignore
    self.session_event(session, RecoverySettled(link.local_handle, tag))
  }
}

///|
fn Endpoint::end_link_with_error(
  self : Endpoint,
  session : SessionState,
  link : LinkState,
  condition : String,
  description : String,
) -> Unit raise AmqpError {
  self.io.check_drop_capacity(session, link)
  let error = Described(
    ULong(0x1dUL),
    List([Symbol(condition), Text(description)]),
  )
  self.queue(
    self.frame(
      0x16,
      [UInt(link.local_handle), Boolean(true), error],
      channel=session.channel,
    ),
  )
  let pending = if link.sender { session.outgoing } else { session.incoming }
  link.failed_deliveries = pending
    .keys()
    .to_array()
    .filter(id => pending[id] == link.local_handle)
  self.io.drop_link(session, link)
  link.closed = true
  link.failed = true
  self.unbind_link(link)
  self.session_event(session, Detached(link.local_handle))
}
