///|
test "receiver must exhaust credit before suspension" {
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
  let remote = b.primary.links[0].local_handle
  assert_eq(a.suspend_readiness(h), SuspendReady)
  assert_eq(b.suspend_readiness(remote), SuspendReady)
  b.grant(remote, 1U)
  assert_eq(b.suspend_readiness(remote), ReceiverHasCredit)
}
