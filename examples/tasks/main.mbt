///|
fn main raise {
  let (application, worker) = @support.pair()
  let (sender, receiver) = @support.sender(
    application, worker, "asset-jobs", "jobs/assets", 41U, 1U,
  )
  let body = Bytes::from_array(Array::makei(5000, i => (i % 251).to_byte()))
  let message = @amqp.Message::data(body, properties=[
    ("job", Text("thumbnail")),
    ("attempt", UInt(1U)),
  ])
  let before = application.session_positions().1
  let id = application.send(sender, message, b"asset-42")
  let frames = application.session_positions().1 - before
  assert_true(frames > 1U)
  assert_eq(application.link_credit(sender), 0U)
  let blocked = try {
    ignore(application.send(sender, message, b"next"))
    false
  } catch {
    _ => true
  }
  assert_true(blocked)
  let encoded = application.drain()
  // A host transport may split any byte boundary.
  for byte in encoded {
    worker.receive(Bytes::from_array([byte]))
  }
  let events = worker.events()
  match events[0] {
    Received(delivery) => {
      assert_eq(delivery.message.data_bytes(), body)
      assert_eq(delivery.tag, b"asset-42")
      worker.acknowledge(delivery.id, Accepted)
    }
    _ => fail("complete job required")
  }
  application.receive(worker.drain())
  assert_true(application.events()[0] is Settled(_, Accepted))
  assert_eq(application.pending(), (0, 0))
  // A failed job can be explicitly released; retry/persistence stays with the host.
  worker.grant(receiver, 1U)
  application.receive(worker.drain())
  ignore(application.events())
  let retry_id = application.send(
    sender,
    @amqp.Message::data(b"retryable"),
    b"asset-43",
  )
  worker.receive(application.drain())
  ignore(worker.events())
  worker.acknowledge(retry_id, Released)
  application.receive(worker.drain())
  assert_true(application.events()[0] is Settled(_, Released))
  assert_eq(retry_id, id + 1U)
  @support.close(application, worker)
  println(
    "tasks: 5000 payload bytes, \{frames} transfer frames, one credit, accepted/released and close verified",
  )
}
