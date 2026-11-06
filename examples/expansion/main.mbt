///|
fn main raise {
  let (sender, receiver) = @support.pair(frame_bytes=512)
  let (handle, _) = @support.sender(sender, receiver, "worker", "tasks", 1U, 1U)
  let original = @amqp.Message::data(b"task")
  let tagged = original.with_application_property("attempt", Some(UInt(1U)))
  assert_eq(tagged.data_bytes(), b"task")
  assert_eq(
    @amqp.encode_message(tagged.with_application_property("attempt", None)),
    @amqp.encode_message(original),
  )
  let before = sender.diagnostics()
  ignore(sender.send(handle, tagged, b"task-1"))
  assert_eq(before.links[0].credit, 1U)
  assert_eq(sender.diagnostics().links[0].outgoing_unsettled, 1)
  assert_true(sender.diagnostics().queued_bytes > 0)
  receiver.receive(sender.drain())
  println(
    "Retry metadata edits preserve payload; detached diagnostics expose credit and unsettled work",
  )
}
