///|
test "asymmetric AMQP idle clocks emit heartbeats and partial frames do not postpone close" {
  let a = Endpoint::new("sender", idle_advertised_ms=20U)
  let b = Endpoint::new("receiver", idle_advertised_ms=40U)
  a.start()
  b.receive_at(a.drain_at(100UL), 100UL)
  a.receive_at(b.drain_at(100UL), 100UL)
  assert_eq(a.idle_delay_ms(119UL), Some(1UL))
  assert_eq(a.poll_idle(119UL), Idle)
  assert_eq(a.poll_idle(120UL), HeartbeatQueued)
  assert_eq(a.idle_delay_ms(121UL), Some(19UL))
  assert_eq(a.poll_idle(121UL), Idle)
  let heartbeat = a.drain_at(121UL)
  b.receive_at(heartbeat, 121UL)
  a.receive_at(heartbeat[:4].to_owned(), 139UL)
  assert_eq(a.poll_idle(140UL), IdleCloseQueued)
  assert_eq(a.poll_idle(141UL), Idle)
  b.receive_at(a.drain_at(141UL), 141UL)
  assert_true(b.is_closed())
  assert_true(b.events().any(event => event is RemoteClosed(Described(_, _))))
}

///|
test "complete inbound frames refresh timeout and clock rollback preserves state" {
  let a = Endpoint::new("sender", idle_advertised_ms=10U)
  let b = Endpoint::new("receiver")
  a.start()
  b.receive_at(a.drain_at(0UL), 0UL)
  a.receive_at(b.drain_at(0UL), 0UL)
  b.heartbeat()
  a.receive_at(b.drain_at(19UL), 19UL)
  assert_true(
    try {
      ignore(a.poll_idle(18UL))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(a.poll_idle(38UL), Idle)
  assert_eq(a.poll_idle(39UL), IdleCloseQueued)
}
