///|
test "idle deadlines distinguish queued work from receive timeout" {
  let a = Endpoint::new("a", idle_advertised_ms=20U)
  let b = Endpoint::new("b", idle_advertised_ms=40U)
  a.start()
  b.receive_at(a.drain_at(100UL), 100UL)
  a.receive_at(b.drain_at(100UL), 100UL)
  assert_eq(a.idle_deadlines(110UL), (Some(30UL), Some(10UL)))
  a.heartbeat()
  assert_eq(a.idle_deadlines(119UL), (Some(21UL), None))
}
