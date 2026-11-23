///|
fn assert_stolen(bytes : Bytes) -> Unit raise {
  let frame = single_wire_frame(bytes).performative.unwrap()
  assert_eq(frame.code, 0x16)
  assert_true(bool_field(frame, 1, false))
  let error = described_list(frame.field(2), 0x1dUL, "amqp:error:list")
  assert_true(error[0] is Symbol("amqp:link:stolen"))
}

///|
test "same-name takeover closes only the old link and leaves another link usable" {
  let shared = RecoveryStore::new()
  let original = Endpoint::new("client")
  let older = Endpoint::new("broker", recovery=Some(shared))
  reconnect_pair(original, older)
  let (old_handle, _) = ready_sender(original, older, "orders")
  let (unrelated, unrelated_remote) = ready_sender(
    original,
    older,
    "metrics",
    handle=42U,
  )
  ignore(older.events())

  let replacement = Endpoint::new("client")
  let newer = Endpoint::new("broker", recovery=Some(shared))
  reconnect_pair(replacement, newer)
  let (new_handle, _) = ready_sender(replacement, newer, "orders")
  let notice = older.drain()
  assert_stolen(notice)
  original.receive(notice)
  older.receive(original.drain())
  assert_true(
    shared.bindings.get(("broker", "client", "orders", false)) is Some(binding) &&
    physical_equal(binding.session, newer.primary),
  )
  assert_false(older.is_failed())
  assert_false(older.is_closed())
  assert_true(older.events().any(e => e is Detached(0U)))
  assert_true(
    try {
      ignore(older.link_credit(0U))
      false
    } catch {
      State(_) => true
      _ => false
    },
  )

  // The old transport can continue carrying an independent link.
  ignore(
    original.send(
      unrelated,
      Message::data(b"same connection"),
      b"old",
      settled=true,
    ),
  )
  older.receive(original.drain())
  let messages = older.events()
  assert_true(
    messages.any(e => e is Received(d) && d.handle == unrelated_remote),
  )
  ignore(
    replacement.send(
      new_handle,
      Message::data(b"new connection"),
      b"new",
      settled=true,
    ),
  )
  newer.receive(replacement.drain())
  assert_true(newer.events().any(e => e is Received(_)))
  assert_true(old_handle != unrelated)
}

///|
test "cross-connection takeover carries receiver state into the replacement Attach" {
  let clients = RecoveryStore::new()
  let brokers = RecoveryStore::new()
  let client = Endpoint::new("client", recovery=Some(clients))
  let old_broker = Endpoint::new("broker", recovery=Some(brokers))
  reconnect_pair(client, old_broker)
  let handle = client.attach(
    "tasks",
    true,
    "jobs",
    recoverable=true,
    receiver_mode=Second,
  )
  old_broker.receive(client.drain())
  client.receive(old_broker.drain())
  old_broker.grant(0U, 1U)
  client.receive(old_broker.drain())
  let id = client.send(handle, Message::data(b"already applied"), b"tag")
  old_broker.receive(client.drain())
  old_broker.acknowledge(id, Accepted)
  ignore(old_broker.drain())
  client.transport_lost()
  assert_eq(brokers.retained_links(), 0)

  let client = Endpoint::new("client", recovery=Some(clients))
  let broker = Endpoint::new("broker", recovery=Some(brokers))
  reconnect_pair(client, broker)
  let handle = client.resume_link("tasks", handle=9U)
  broker.receive(client.drain())
  let reply = broker.drain()
  let map = decode_unsettled(
    single_wire_frame(reply).performative.unwrap().field(7),
  )
  assert_eq(Outcome::from_value(map[b"tag"]), Accepted)
  client.receive(reply)
  assert_stolen(old_broker.drain())
  assert_eq(old_broker.pending(), (0, 0))
  old_broker.transport_lost()
  assert_eq(brokers.retained_links(), 0)
  broker.grant(0U, 1U)
  client.receive(broker.drain())
  assert_eq(client.resumable_deliveries(handle), [b"tag"])
  ignore(client.resume_delivery(handle, b"tag"))
  broker.receive(client.drain())
  assert_false(broker.events().any(e => e is Received(_)))
  assert_eq(client.pending(), (0, 0))
  assert_eq(broker.recovery_status(0U).retained, 0)
}

///|
test "the replacement sender inherits unsettled state when its receiver reconnects" {
  let clients = RecoveryStore::new()
  let brokers = RecoveryStore::new()
  let receiver = Endpoint::new("consumer", recovery=Some(clients))
  let sender = Endpoint::new("broker", recovery=Some(brokers))
  reconnect_pair(receiver, sender)
  let old = receiver.attach(
    "results",
    false,
    "jobs",
    recoverable=true,
    receiver_mode=Second,
  )
  sender.receive(receiver.drain())
  receiver.receive(sender.drain())
  receiver.grant(old, 1U)
  sender.receive(receiver.drain())
  let id = sender.send(0U, Message::data(b"result"), b"result-id")
  receiver.receive(sender.drain())
  receiver.acknowledge(id, Accepted)
  ignore(receiver.drain())
  receiver.transport_lost()

  let receiver = Endpoint::new("consumer", recovery=Some(clients))
  let newer = Endpoint::new("broker", recovery=Some(brokers))
  reconnect_pair(receiver, newer)
  let handle = receiver.resume_link("results", handle=6U)
  newer.receive(receiver.drain())
  receiver.receive(newer.drain())
  assert_stolen(sender.drain())
  receiver.grant(handle, 1U)
  newer.receive(receiver.drain())
  assert_eq(newer.resumable_deliveries(0U), [b"result-id"])
  ignore(newer.resume_delivery(0U, b"result-id"))
  receiver.receive(newer.drain())
  assert_eq(receiver.recovery_status(handle).retained, 0)
  assert_eq(newer.recovery_status(0U).retained, 0)
}

///|
test "takeover preflights the old output queue before changing owners" {
  let shared = RecoveryStore::new()
  let first = Endpoint::new("client")
  let old = Endpoint::new("broker", recovery=Some(shared))
  reconnect_pair(first, old)
  let (_, handle) = ready_sender(first, old, "jobs")
  old.io.queued_bytes = old.limits.max_value_bytes
  let candidate = Endpoint::new("client")
  let latest = Endpoint::new("broker", recovery=Some(shared))
  reconnect_pair(candidate, latest)
  ignore(candidate.attach("jobs", true, "jobs"))
  assert_true(
    try {
      latest.receive(candidate.drain())
      false
    } catch {
      Limit(_) => true
      _ => false
    },
  )
  assert_true(old.link_credit(handle) > 0U)
  assert_true(old.io.outbound.is_empty())
  old.io.queued_bytes = 0
  assert_true(
    shared.bindings.get(("broker", "client", "jobs", false)) is Some(binding) &&
    physical_equal(binding.link, old.primary.local_link(handle)),
  )
}
