///|
test "send planning does not consume credit or delivery IDs and matches emitted frames" {
  let limits = { ..@moonamqp1.Limits::default(), max_frame_bytes: 512, }
  let a = @moonamqp1.Endpoint::new("producer", limits~)
  let b = @moonamqp1.Endpoint::new("consumer", limits~)
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  let handle = a.attach("jobs", true, "work")
  b.receive(a.drain())
  a.receive(b.drain())
  let mut receiver = 0U
  for event in b.events() {
    if event is LinkReady(h, _, false) {
      receiver = h
    }
  }
  b.grant(receiver, 1U)
  a.receive(b.drain())
  let m = @moonamqp1.Message::data(Bytes::make(5000, b'x'))
  let before = a.session_positions()
  let plan = a.plan_send(handle, m, b"task")
  assert_true(plan.frames > 1)
  assert_eq(a.plan_send(handle, m, b"task").wire_bytes, plan.wire_bytes)
  assert_eq(a.session_positions(), before)
  assert_eq(a.link_credit(handle), 1U)
  assert_eq(a.drain().length(), 0)
  let id = a.send(handle, m, b"task")
  assert_eq(id, 0U)
  let bytes = a.drain()
  assert_eq(bytes.length(), plan.wire_bytes)
  let stream = @moonamqp1.FrameStream::new(expect_header=false, limits~)
  assert_eq(stream.feed(bytes).length(), plan.frames)
  assert_true(
    try {
      ignore(a.plan_send(handle, m, b"next"))
      false
    } catch {
      _ => true
    },
  )
}
