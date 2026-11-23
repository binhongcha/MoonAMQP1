///|
fn open_pair(
  a : @amqp.Endpoint,
  b : @amqp.Endpoint,
) -> Unit raise @amqp.AmqpError {
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
}

///|
fn trace(label : String, bytes : Bytes) -> Bytes {
  let digits = "0123456789abcdef".to_array()
  let hex = StringBuilder(size_hint=bytes.length() * 2)
  for byte in bytes {
    let value = byte.to_int()
    hex.write_char(digits[value >> 4])
    hex.write_char(digits[value & 15])
  }
  println(label + "=" + hex.to_string())
  bytes
}

///|
fn main raise {
  let limits = { ..@amqp.Limits::default(), max_frame_bytes: 620, }
  let producer_state = @amqp.RecoveryStore::new()
  let worker_state = @amqp.RecoveryStore::new()
  let a = @amqp.Endpoint::new(
    "producer",
    limits~,
    recovery=Some(producer_state),
  )
  let b = @amqp.Endpoint::new("worker", limits~, recovery=Some(worker_state))
  open_pair(a, b)
  let h = a.attach(
    "tasks",
    true,
    String::make(194, 'x'),
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
  let job_a = @amqp.Message::data(b"done")
  let data = Bytes::from_array(Array::make(1400, b'x'))
  let job_b = @amqp.Message::data(data)
  ignore(a.send(h, job_a, tag_a))
  b.receive(a.drain())
  b.acknowledge(0U, Accepted)
  ignore(b.drain())
  ignore(a.send(h, job_b, tag_b))
  let stream = @amqp.FrameStream::new(expect_header=false, limits~)
  let old = stream.feed(a.drain())
  guard old[0] is AmqpFrame(first) else { fail("expected first task fragment") }
  b.receive(trace("prefix", @amqp.encode_frame(first, limits~)))
  a.transport_lost()
  b.transport_lost()
  assert_eq(producer_state.retained_links(), 1)
  assert_eq(worker_state.retained_links(), 1)

  let a = @amqp.Endpoint::new(
    "producer",
    limits~,
    recovery=Some(producer_state),
  )
  let b = @amqp.Endpoint::new("worker", limits~, recovery=Some(worker_state))
  open_pair(a, b)
  let h = a.resume_link("tasks", handle=11U)
  b.receive(trace("a_attach1", a.drain()))
  let receiver_attach = b.drain()
  let receiver_frames = @amqp.FrameStream::new(expect_header=false).feed(
    receiver_attach,
  )
  guard receiver_frames[0] is AmqpFrame(frame) else {
    fail("missing receiver Attach")
  }
  let receiver_handle = match frame.performative.unwrap().field(1) {
    UInt(handle) => handle
    _ => fail("missing receiver handle")
  }
  a.receive(trace("b_attach1", receiver_attach))
  b.grant(receiver_handle, 1U)
  a.receive(b.drain())
  assert_eq(a.resumable_deliveries(h), [tag_a])
  ignore(a.resume_delivery(h, tag_a))
  b.receive(trace("a_recover_a", a.drain()))
  assert_false(b.events().any(event => event is Received(_)))
  a.suspend_link(h)
  b.receive(trace("a_detach", a.drain()))
  a.receive(trace("b_detach", b.drain()))

  let h = a.resume_link("tasks", handle=12U)
  b.receive(trace("a_attach2", a.drain()))
  a.receive(trace("b_attach2", b.drain()))
  b.grant(receiver_handle, 2U)
  a.receive(b.drain())
  assert_eq(a.resumable_deliveries(h), [tag_b])
  let id = a.resume_delivery(h, tag_b)
  b.receive(trace("a_recover_b", a.drain()))
  let deliveries = b.events().filter(event => event is Received(_))
  guard deliveries is [Received(delivery)] else {
    fail("recovered payload must be delivered once")
  }
  assert_eq(delivery.id, id)
  assert_eq(delivery.message.data_bytes(), data)
  b.acknowledge(id, Accepted)
  a.receive(trace("b_disposition", b.drain()))
  b.receive(trace("a_disposition", a.drain()))
  assert_eq(a.pending(), (0, 0))
  assert_eq(b.pending(), (0, 0))
  ignore(a.send(h, job_a, b"next", settled=true))
  b.receive(trace("new_delivery", a.drain()))
  assert_true(b.events().any(event => event is Received(_)))
  println(
    "Recovered two pending deliveries; subsequent delivery accepted by the receiver.",
  )
}
