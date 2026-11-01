///|
fn main raise {
  let (sender, receiver) = @support.pair(frame_bytes=512)
  let (handle, _) = @support.sender(sender, receiver, "jobs", "work", 1U, 1U)
  let m = @amqp.Message::new([
    { descriptor: 0x70, body: List([Null, Null, UInt(5000U)]), },
    { descriptor: 0x75, body: Binary(Bytes::make(5000, b'x')), },
  ])
  assert_true(!m.expiry_status(1000L, 2000L).expired)
  let plan = sender.plan_send(handle, m, b"job-1")
  assert_true(plan.frames > 1)
  assert_eq(sender.link_credit(handle), 1U)
  ignore(sender.send(handle, m, b"job-1"))
  let bytes = sender.drain()
  assert_eq(bytes.length(), plan.wire_bytes)
  receiver.receive(bytes)
  assert_true(m.expiry_status(1000L, 6000L).expired)
  println(
    "Task preflight preserves credit; fragmented transfer matches wire plan; expired jobs are explicit",
  )
}
