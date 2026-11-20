///|
test "bounded drain preserves wire order and rejects undersized transport capacity atomically" {
  let original = Endpoint::new("a")
  original.start()
  let wire = original.drain()
  let endpoint = Endpoint::new("a")
  endpoint.start()
  let rejected = try {
    ignore(endpoint.drain_bounded_at(7, 20UL))
    false
  } catch {
    Limit(_) => true
    _ => false
  }
  assert_true(rejected)
  assert_eq(endpoint.drain_bounded_at(4096, 10UL), wire)
  let remote = Endpoint::new("b")
  remote.receive(wire)
  endpoint.receive_at(remote.drain(), 10UL)
  endpoint.heartbeat()
  endpoint.heartbeat()
  let first = endpoint.drain_bounded_at(8, 11UL)
  let second = endpoint.drain_bounded_at(8, 12UL)
  assert_eq(first.length(), 8)
  assert_eq(first, second)
  assert_true(endpoint.drain_bounded_at(8, 13UL).is_empty())
}
