///|
test "settled link suspends, reattaches on a new handle and gets fresh credit" {
  let a = Endpoint::new("a")
  let b = Endpoint::new("b")
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  let h = a.attach("job", true, "jobs", handle=3U)
  b.receive(a.drain())
  a.receive(b.drain())
  let bh = b.primary.links[0].local_handle
  b.grant(bh, 1U)
  a.receive(b.drain())
  ignore(a.send(h, Message::data(b"first"), b"tag"))
  b.receive(a.drain())
  assert_true(
    try {
      a.suspend_link(h)
      false
    } catch {
      _ => true
    },
  )
  b.acknowledge(0U, Accepted)
  a.receive(b.drain())
  a.suspend_link(h)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_true(a.events().any(e => e is LinkSuspended("job", 3U)))
  assert_true(b.events().any(e => e is LinkSuspended("job", _)))
  let new_handle = a.resume_link("job", handle=11U)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(new_handle, 11U)
  assert_eq(a.link_credit(new_handle), 0U)
  b.grant(b.primary.links[0].local_handle, 1U)
  a.receive(b.drain())
  ignore(a.send(new_handle, Message::data(b"second"), b"tag2"))
  b.receive(a.drain())
  assert_true(
    b
    .events()
    .any(e => {
      match e {
        Received(d) => d.id == 1U && d.message.data_bytes() == b"second"
        _ => false
      }
    }),
  )
}
