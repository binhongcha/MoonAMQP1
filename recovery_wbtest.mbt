///|
fn reconnect_pair(left : Endpoint, right : Endpoint) -> Unit raise AmqpError {
  left.start()
  right.receive(left.drain())
  left.receive(right.drain())
  left.begin()
  right.receive(left.drain())
  left.receive(right.drain())
  ignore(left.events())
  ignore(right.events())
}

///|
fn single_wire_frame(bytes : Bytes) -> Frame raise AmqpError {
  let frames = FrameStream::new(expect_header=false).feed(bytes)
  match frames {
    [AmqpFrame(frame)] => frame
    _ => raise State("expected one wire frame")
  }
}

///|
test "cross-connection incomplete maps preserve the omitted tag and resume only missing bytes" {
  let store_a = RecoveryStore::new()
  let store_b = RecoveryStore::new()
  let a = Endpoint::new("producer", recovery=Some(store_a))
  let b = Endpoint::new("worker", recovery=Some(store_b))
  reconnect_pair(a, b)
  let address = String::make(194, 'x')
  let old = a.attach(
    "tasks",
    true,
    address,
    handle=3U,
    receiver_mode=Second,
    recoverable=true,
  )
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(0U, 2U)
  a.receive(b.drain())
  let tag_a = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  let tag_b = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  let message_a = Message::data(b"already accepted")
  let body_b = Bytes::from_array(Array::make(70, b'x'))
  let message_b = Message::data(body_b)
  ignore(a.send(old, message_a, tag_a))
  b.receive(a.drain())
  b.acknowledge(0U, Accepted)
  ignore(b.drain()) // The old connection ends before this disposition reaches the sender.
  ignore(a.send(old, message_b, tag_b))
  let original = single_wire_frame(a.drain())
  let original_state = original.performative.unwrap()
  let fields = [
    UInt(old),
    original_state.field(1),
    Binary(tag_b),
    UInt(0U),
    Boolean(false),
    Boolean(true),
  ]
  b.receive(b.frame(0x14, fields, payload=original.payload[:32].to_owned()))
  a.transport_lost()
  b.transport_lost()
  assert_eq(store_a.retained_links(), 1)
  assert_eq(store_b.retained_links(), 1)
  let fresh_limits = { ..Limits::default(), max_frame_bytes: 620, }
  let a = Endpoint::new("producer", recovery=Some(store_a), limits=fresh_limits)
  let b = Endpoint::new("worker", recovery=Some(store_b), limits=fresh_limits)
  reconnect_pair(a, b)
  let h1 = a.resume_link("tasks", handle=11U)
  let first_attach = a.drain()

  let attach_from_a = single_wire_frame(first_attach).performative.unwrap()
  assert_eq(decode_unsettled(attach_from_a.field(7)).length(), 2)
  assert_eq(bool_field(attach_from_a, 8, false), false)
  b.receive(first_attach)
  let attach_from_b = b.drain()

  let receiver_frame = single_wire_frame(attach_from_b).performative.unwrap()
  let first_map = decode_unsettled(receiver_frame.field(7))
  assert_true(bool_field(receiver_frame, 8, false))
  assert_true(first_map.contains(tag_a))
  assert_false(first_map.contains(tag_b))
  a.receive(attach_from_b)
  assert_eq(store_a.retained_links(), 0)
  assert_eq(store_b.retained_links(), 0)
  b.grant(0U, 2U)
  a.receive(b.drain())
  assert_eq(a.resumable_deliveries(h1), [tag_a])
  assert_true(a.unseen_deliveries(h1).is_empty())
  assert_true(
    try {
      ignore(a.send(h1, message_a, b"new"))
      false
    } catch {
      State(_) => true
      _ => false
    },
  )
  assert_eq(a.resume_delivery(h1, tag_a), 0U)
  let outcome_wire = a.drain()
  let outcome_frame = single_wire_frame(outcome_wire)
  let outcome = outcome_frame.performative.unwrap()
  assert_true(bool_field(outcome, 8, false))
  assert_true(bool_field(outcome, 4, false))
  assert_true(outcome_frame.payload.is_empty())
  assert_eq(Outcome::from_value(outcome.field(7)), Accepted)
  b.receive(outcome_wire)
  assert_false(b.events().any(event => event is Received(_)))

  a.suspend_link(h1)
  b.receive(a.drain())
  a.receive(b.drain())
  let h2 = a.resume_link("tasks", handle=12U)
  b.receive(a.drain())
  let round_two = b.drain()
  let second_map = single_wire_frame(round_two).performative.unwrap()
  assert_false(bool_field(second_map, 8, false))
  assert_eq(
    received_fields(decode_unsettled(second_map.field(7))[tag_b]),
    Some((0U, 32UL)),
  )
  a.receive(round_two)
  b.grant(0U, 2U)
  a.receive(b.drain())
  assert_eq(a.resume_delivery(h2, tag_b), 1U)
  let remaining = a.drain()
  let resumed = single_wire_frame(remaining)
  assert_eq(resumed.payload, original.payload[32:].to_owned())
  assert_eq(
    received_fields(resumed.performative.unwrap().field(7)),
    Some((0U, 32UL)),
  )
  b.receive(remaining)
  let delivered = b.events().filter(event => event is Received(_))
  assert_eq(delivered.length(), 1)
  guard delivered[0] is Received(delivery) else { fail("missing restored job") }
  assert_eq(delivery.message.data_bytes(), body_b)
  assert_eq(delivery.id, 1U)
  b.acknowledge(delivery.id, Accepted)
  let worker_result = b.drain()
  let receiving_disposition = single_wire_frame(worker_result).performative.unwrap()
  assert_true(bool_field(receiving_disposition, 0, false))
  assert_false(bool_field(receiving_disposition, 3, false))
  a.receive(worker_result)
  let sender_result = a.drain()
  let sending_disposition = single_wire_frame(sender_result).performative.unwrap()
  assert_false(bool_field(sending_disposition, 0, false))
  assert_true(bool_field(sending_disposition, 3, false))
  b.receive(sender_result)
  assert_eq(a.pending(), (0, 0))
  assert_eq(b.pending(), (0, 0))
  assert_true(a.primary.links[0].records.is_empty())
  assert_true(b.primary.links[0].records.is_empty())
  ignore(a.send(h2, message_a, b"next", settled=true))
  b.receive(a.drain())
  assert_true(b.events().any(event => event is Received(_)))
}

///|
test "receiver rejects a new delivery during an incomplete round with link error" {
  let (a, b) = connected_pair()
  let (sender, receiver) = ready_sender(a, b, "partial-map")
  let link = b.primary.local_link(receiver)
  link.sent_incomplete = true
  assert_true(link.credit > 0U)
  let payload = encode_message(Message::data(b"unexpected"))
  b.receive(
    peer_wire(
      a,
      0x14,
      [UInt(sender), UInt(0U), Binary(b"new"), UInt(0U)],
      payload~,
    ),
  )
  let response = single_wire_frame(b.drain()).performative.unwrap()
  assert_eq(response.code, 0x16)
  assert_false(response.field(2) is Null)
  assert_false(b.is_failed())
}

///|
test "received positions distinguish boundaries from offsets in later encoded sections" {
  let message = Message::data(b"value", properties=[("unit", Text("kW"))])
  let bytes = encode_message(message)
  let limits = Limits::default()
  let first = encode_value(section_value(message.sections()[0]))
  let boundary = first.length()
  let cut = bytes[:boundary + 2].to_owned()
  assert_eq(received_fields(prefix_state(cut, limits)), Some((1U, 2UL)))
  assert_eq(recovery_offset(bytes, 1U, 2UL, limits), boundary + 2)
  assert_eq(recovery_offset(bytes, 0U, boundary.to_uint64(), limits), boundary)
}

///|
test "same-session disposition settles a suspended link without reviving the tag" {
  let (a, b) = connected_pair()
  let h = a.attach(
    "retained",
    true,
    "dest",
    recoverable=true,
    receiver_mode=Second,
  )
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(0U, 1U)
  a.receive(b.drain())
  let id = a.send(h, Message::data(b"job"), b"tag")
  b.receive(a.drain())
  a.suspend_link(h)
  b.receive(a.drain())
  a.receive(b.drain())
  b.acknowledge(id, Accepted)
  a.receive(b.drain())
  b.receive(a.drain())
  let h = a.resume_link("retained", handle=9U)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(a.recovery_status(h).retained, 0)
  assert_eq(b.recovery_status(0U).retained, 0)
  assert_true(a.resumable_deliveries(h).is_empty())
}

///|
test "source-only message can be released for a new delivery after a complete receiver map" {
  let store_a = RecoveryStore::new()
  let store_b = RecoveryStore::new()
  let a = Endpoint::new("a", recovery=Some(store_a))
  let b = Endpoint::new("b", recovery=Some(store_b))
  reconnect_pair(a, b)
  let handle = a.attach("unseen", true, "dest", recoverable=true)
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(0U, 1U)
  a.receive(b.drain())
  ignore(a.send(handle, Message::data(b"retry payload"), b"tag"))
  ignore(a.drain())
  a.transport_lost()
  b.transport_lost()
  let a = Endpoint::new("a", recovery=Some(store_a))
  let b = Endpoint::new("b", recovery=Some(store_b))
  reconnect_pair(a, b)
  let handle = a.resume_link("unseen", handle=9U)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(a.unseen_deliveries(handle), [b"tag"])
  let message = a.take_unseen(handle, b"tag").unwrap()
  assert_eq(message.data_bytes(), b"retry payload")
  assert_eq(a.recovery_status(handle).retained, 0)
  b.grant(0U, 1U)
  a.receive(b.drain())
  ignore(a.send(handle, message, b"tag"))
  let retry = a.drain()
  assert_false(
    bool_field(single_wire_frame(retry).performative.unwrap(), 8, false),
  )
  b.receive(retry)
  assert_true(b.events().any(event => event is Received(_)))
}

///|
test "unsupported transaction coordinator declines Attach and leaves session usable" {
  let (a, b) = connected_pair()
  let coordinator = Described(ULong(0x30UL), List([]))
  b.receive(
    peer_wire(a, 0x12, [
      Text("transaction"),
      UInt(2U),
      Boolean(false),
      Null,
      Null,
      source("jobs"),
      coordinator,
      Null,
      Null,
      UInt(0U),
    ]),
  )
  let replies = FrameStream::new(expect_header=false).feed(b.drain())
  guard replies is [AmqpFrame(decline), AmqpFrame(detach)] else {
    fail("reply must Attach with null target and then Detach")
  }
  assert_eq(decline.performative.unwrap().code, 0x12)
  assert_true(decline.performative.unwrap().field(6) is Null)
  assert_eq(detach.performative.unwrap().code, 0x16)
  assert_false(b.is_failed())
  let (_, _) = ready_sender(a, b, "ordinary")
}

///|
test "unsettled map rejects duplicate binary keys" {
  let invalid = MapValue([
    (Binary(b"x"), Null),
    (Binary(b"x"), Accepted.value()),
  ])
  assert_true(
    try {
      ignore(decode_unsettled(invalid))
      false
    } catch {
      Invalid(_, _) => true
      _ => false
    },
  )
}

///|
test "receiver-only record settles after the sender's complete empty map" {
  let store_a = RecoveryStore::new()
  let store_b = RecoveryStore::new()
  let a = Endpoint::new("a", recovery=Some(store_a))
  let b = Endpoint::new("b", recovery=Some(store_b))
  reconnect_pair(a, b)
  let h = a.attach(
    "receiver-only",
    true,
    "dest",
    recoverable=true,
    receiver_mode=Second,
  )
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(0U, 1U)
  a.receive(b.drain())
  let id = a.send(h, Message::data(b"job"), b"tag")
  b.receive(a.drain())
  b.acknowledge(id, Accepted)
  a.receive(b.drain())
  assert_eq(a.pending(), (0, 0))
  assert_eq(b.pending(), (0, 1))
  ignore(a.drain())
  a.transport_lost()
  b.transport_lost()
  let a = Endpoint::new("a", recovery=Some(store_a))
  let b = Endpoint::new("b", recovery=Some(store_b))
  reconnect_pair(a, b)
  let h = a.resume_link("receiver-only", handle=9U)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_true(a.resumable_deliveries(h).is_empty())
  assert_eq(b.recovery_status(0U).retained, 0)
  assert_true(b.events().any(event => event is RecoverySettled(0U, b"tag")))
}

///|
test "on terminal conflict the sender restates its outcome and the receiver echoes settlement" {
  let (a, b) = connected_pair()
  let h = a.attach(
    "conflict",
    true,
    "dest",
    recoverable=true,
    receiver_mode=Second,
  )
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(0U, 1U)
  a.receive(b.drain())
  let id = a.send(h, Message::data(b"job"), b"tag")
  b.receive(a.drain())
  let sender_entry = a.primary.links[0].records[b"tag"]
  let receiver_entry = b.primary.links[0].records[b"tag"]
  sender_entry.state = Accepted.value()
  receiver_entry.state = Released.value()
  a.suspend_link(h)
  b.receive(a.drain())
  a.receive(b.drain())
  let h = a.resume_link("conflict", handle=9U)
  b.receive(a.drain())
  a.receive(b.drain())
  let worker_handle = b.primary.links[0].local_handle
  b.grant(worker_handle, 1U)
  a.receive(b.drain())
  let restored = a.resume_delivery(h, b"tag")
  assert_eq(restored, id + 1U)
  let wire = a.drain()
  let transfer = single_wire_frame(wire)
  assert_true(transfer.payload.is_empty())
  assert_false(bool_field(transfer.performative.unwrap(), 4, false))
  assert_eq(
    Outcome::from_value(transfer.performative.unwrap().field(7)),
    Accepted,
  )
  ignore(b.events())
  b.receive(wire)
  assert_false(b.events().any(event => event is Received(_)))
  let reply = b.drain()
  let disposition = single_wire_frame(reply).performative.unwrap()
  assert_true(bool_field(disposition, 3, false))
  assert_eq(Outcome::from_value(disposition.field(4)), Accepted)
  a.receive(reply)
  assert_eq(a.recovery_status(h).retained, 0)
  assert_eq(b.recovery_status(worker_handle).retained, 0)
}

///|
test "unsettled summaries rotate when one entry fits and neither delivery has settled" {
  let limits = { ..Limits::default(), max_frame_bytes: 620, }
  let a = Endpoint::new("receiver", limits~)
  a.peer_frame_max = 620
  let link = link_state("tasks", 0U, false, String::make(194, 'x'))
  link.recoverable = true
  let tag_a = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  let tag_b = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  link.records[tag_a] = {
    id: None,
    state: Accepted.value(),
    bytes: b"",
    delivered: true,
  }
  link.records[tag_b] = {
    id: None,
    state: received_state(0U, 32UL),
    bytes: b"",
    delivered: false,
  }
  let (first, incomplete, cursor) = a.attach_recovery_frame(a.primary, link, 1U)
  assert_true(incomplete)
  let offered = decode_unsettled(
    single_wire_frame(first).performative.unwrap().field(7),
  )
  assert_true(offered.contains(tag_a))
  assert_false(offered.contains(tag_b))
  link.recovery_cursor = cursor
  let (second, incomplete, _) = a.attach_recovery_frame(a.primary, link, 2U)
  assert_true(incomplete)
  let offered = decode_unsettled(
    single_wire_frame(second).performative.unwrap().field(7),
  )
  assert_false(offered.contains(tag_a))
  assert_true(offered.contains(tag_b))
  assert_eq(link.records.length(), 2)
}

///|
test "unsupported transactional acquisition detaches its link and preserves the session" {
  let (sender, receiver) = connected_pair()
  let (_, remote_handle) = ready_sender(sender, receiver, "non-transactional")
  let flow = [
    UInt(0U),
    UInt(32U),
    UInt(0U),
    UInt(32U),
    UInt(remote_handle),
    UInt(0U),
    UInt(1U),
    Null,
    Null,
    Null,
    MapValue([(Symbol("txn-id"), Binary(b"transaction"))]),
  ]
  sender.receive(peer_wire(receiver, 0x13, flow))
  let detach = single_wire_frame(sender.drain()).performative.unwrap()
  assert_eq(detach.code, 0x16)
  let error = described_list(detach.field(2), 0x1dUL, "amqp:error:list")
  assert_true(error[0] is Symbol("amqp:not-implemented"))
  assert_false(sender.is_failed())
  ignore(sender.attach("ordinary-after-transaction", true, "jobs"))
}

///|
test "interrupted replay retains the prefix of an already exposed unsettled delivery" {
  let bytes = encode_message(Message::data(b"visible before disconnect"))
  let link = link_state("tasks", 0U, false, "jobs")
  let entry : RecoveryDelivery = {
    id: Some(0U),
    state: received_state(1U, 0UL),
    bytes: b"",
    delivered: true,
  }
  link.records[b"job"] = entry
  link.partial = Some({
    id: 0U,
    tag: b"job",
    settled: false,
    receiver_mode: Second,
    bytes: bytes.to_array(),
    fragments: 1,
    skip: 3,
    ignored: false,
  })
  link.save_partial(Limits::default())
  assert_eq(entry.bytes, bytes)
  assert_eq(received_fields(entry.state), Some((1U, 0UL)))
  assert_true(entry.delivered)
}
