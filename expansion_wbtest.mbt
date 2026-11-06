///|
test "property edits preserve original message and distinguish removal from null" {
  let m = Message::new([
    { descriptor: 0x72, body: MapValue([(Symbol("trace"), Text("t"))]), },
    { descriptor: 0x75, body: Binary(b"a"), },
    { descriptor: 0x75, body: Binary(b"b"), },
    { descriptor: 0x78, body: MapValue([]), },
  ])
  let before = encode_message(m)
  let e = m.with_application_property("id", Some(ULong(18446744073709551615UL)))
  assert_eq(
    encode_value(
      e.with_application_property("id", Some(Null)).sections()[1].body,
    ),
    encode_value(MapValue([(Text("id"), Null)])),
  )
  assert_eq(e.data_bytes(), b"ab")
  assert_eq(encode_message(m), before)
  assert_eq(encode_message(e.with_application_property("id", None)), before)
  assert_true(
    try {
      ignore(m.with_application_property("x", Some(List([]))))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(
        m.with_application_property("x", Some(Text("large")), limits={
          ..Limits::default(),
          max_message_bytes: 1,
        }),
      )
      false
    } catch {
      _ => true
    },
  )
}

///|
test "diagnostics retain detached link credit pending and event snapshots" {
  let a = Endpoint::new("a")
  let b = Endpoint::new("b")
  assert_true(!a.diagnostics().session_ready)
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  ignore(a.events())
  ignore(b.events())
  let sender = a.attach("work", true, "jobs")
  b.receive(a.drain())
  a.receive(b.drain())
  let receiver = match b.events()[0] {
    LinkReady(h, _, _) => h
    _ => fail("link")
  }
  b.grant(receiver, 1U)
  a.receive(b.drain())
  ignore(a.events())
  let idle = a.diagnostics()
  let id = a.send(sender, Message::data(b"job"), b"tag")
  let sent = a.diagnostics()
  assert_eq(sent.links[0].outgoing_unsettled, 1)
  assert_true(sent.queued_bytes > 0)
  assert_eq(idle.links[0].credit, 1U)
  sent.links.clear()
  assert_eq(a.diagnostics().links.length(), 1)
  b.receive(a.drain())
  ignore(b.events())
  b.acknowledge(id, Accepted)
  a.receive(b.drain())
  assert_eq(a.diagnostics().links[0].outgoing_unsettled, 0)
  assert_eq(a.diagnostics().queued_events, 1)
  assert_eq(a.events().length(), 1)
}
