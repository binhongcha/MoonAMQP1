///|
test "suspended link inventory follows the detach and reattach handshake" {
  let a = Endpoint::new("a")
  let b = Endpoint::new("b")
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  let h = a.attach("jobs", true, "queue")
  b.receive(a.drain())
  a.receive(b.drain())
  a.suspend_link(h)
  assert_true(a.suspended_links().is_empty())
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(a.suspended_links(), [
    { name: "jobs", address: "queue", sender: true, delivery_count: 0U, },
  ])
  ignore(a.resume_link("jobs"))
  assert_true(a.suspended_links().is_empty())
}
