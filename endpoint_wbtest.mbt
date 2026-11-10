///|
fn connected_pair(
  window~ : UInt32 = 32U,
  frame_bytes~ : Int32 = 512,
) -> (Endpoint, Endpoint) raise AmqpError {
  let limits = { ..Limits::default(), max_frame_bytes: frame_bytes, }
  let a = Endpoint::new("producer", channel=3, window~, limits~)
  let b = Endpoint::new("consumer", channel=17, window~, limits~)
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  ignore(a.events())
  ignore(b.events())
  (a, b)
}

///|
fn ready_sender(
  a : Endpoint,
  b : Endpoint,
  name : String,
  handle~ : UInt32 = 41U,
  credit~ : UInt32 = 4U,
) -> (UInt32, UInt32) raise AmqpError {
  let sender = a.attach(name, true, "jobs", handle~)
  b.receive(a.drain())
  a.receive(b.drain())
  let mut receiver = 999U
  for event in b.events() {
    if event is LinkReady(handle, _, false) {
      receiver = handle
    }
  }
  b.grant(receiver, credit)
  a.receive(b.drain())
  ignore(a.events())
  (sender, receiver)
}

///|
test "multi-frame message consumes one credit but multiple transfer IDs then settles" {
  let (a, b) = connected_pair()
  let (sender, receiver) = ready_sender(a, b, "jobs", credit=1U)
  let body = Bytes::from_array(Array::makei(2000, i => (i % 251).to_byte()))
  let before = a.session_positions().1
  let id = a.send(sender, Message::data(body), b"job-1")
  assert_true(a.session_positions().1 - before > 1U)
  assert_eq(a.link_credit(sender), 0U)
  let wire = a.drain()
  for byte in wire {
    b.receive(Bytes::from_array([byte]))
  }
  let received = b.events()
  assert_eq(received.length(), 1)
  match received[0] {
    Received(delivery) => {
      assert_eq(delivery.handle, receiver)
      assert_eq(delivery.id, id)
      assert_eq(delivery.message.data_bytes(), body)
      b.acknowledge(id, Accepted)
    }
    _ => fail("delivery event required")
  }
  a.receive(b.drain())
  assert_true(a.events()[0] is Settled(0U, Accepted))
  assert_eq(a.pending(), (0, 0))
  assert_eq(b.pending(), (0, 0))
}

///|
test "zero-credit sibling link stays blocked while another link sends" {
  let (a, b) = connected_pair()
  let (blocked, _) = ready_sender(a, b, "slow", handle=8U, credit=0U)
  let (fast, _) = ready_sender(a, b, "fast", handle=9U, credit=1U)
  let message = Message::data(b"sample")
  let before = a.session_positions()
  assert_true(
    try {
      ignore(a.send(blocked, message, b"slow"))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(a.session_positions(), before)
  ignore(a.send(fast, message, b"fast", settled=true))
  b.receive(a.drain())
  assert_true(b.events()[0] is Received(_))
  assert_eq(a.pending(), (0, 0))
}

///|
test "insufficient session capacity leaves queued data IDs and credit unchanged" {
  let (a, b) = connected_pair(window=1U)
  let (handle, _) = ready_sender(a, b, "large", credit=1U)
  let message = Message::data(Bytes::from_array(Array::make(1800, 0x42)))
  assert_true(
    try {
      ignore(a.send(handle, message, b"x"))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(a.link_credit(handle), 1U)
  assert_eq(a.drain(), b"")
  assert_eq(a.session_positions().1, 0U)
  a.refresh_windows(32U, 32U)
  b.receive(a.drain())
  b.refresh_windows(32U, 32U)
  a.receive(b.drain())
  assert_eq(a.send(handle, message, b"x"), 0U)
  b.receive(a.drain())
  assert_true(b.events()[0] is Received(_))
}

///|
test "heartbeat detach close and EOF complete without leaked pending deliveries" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "life")
  a.heartbeat()
  b.receive(a.drain())
  assert_eq(b.events().length(), 0)
  a.detach(handle)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_true(a.events()[0] is Detached(_))
  a.close()
  b.receive(a.drain())
  a.receive(b.drain())
  assert_true(a.is_closed())
  assert_true(b.is_closed())
  a.finish_input()
  b.finish_input()
  assert_true(
    try {
      a.begin()
      false
    } catch {
      _ => true
    },
  )
}

///|
test "session end handshakes without impersonating a connection close" {
  let (a, b) = connected_pair()
  a.end_session()
  b.receive(a.drain())
  a.receive(b.drain())
  assert_false(a.is_closed())
  assert_true(a.events()[0] is SessionEnded(_))
  assert_true(b.events()[0] is SessionEnded(_))
  assert_true(
    try {
      ignore(a.attach("late", true, "q"))
      false
    } catch {
      _ => true
    },
  )
  a.close()
  b.receive(a.drain())
  a.receive(b.drain())
  assert_true(a.is_closed())
}

///|
test "detaching a link exposes unresolved delivery loss and releases pending slots" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "pending")
  ignore(a.send(handle, Message::data(b"job"), b"tag"))
  b.receive(a.drain())
  ignore(b.events())
  a.detach(handle)
  assert_eq(a.pending(), (0, 0))
  assert_true(a.events()[0] is UnsettledLost(0U, true))
  b.receive(a.drain())
  assert_eq(b.pending(), (0, 0))
  assert_true(b.events()[0] is UnsettledLost(0U, false))
}

///|
test "aggregate completed-message event bytes are bounded until host drains events" {
  let (a, b) = connected_pair(frame_bytes=8192)
  let (handle, _) = ready_sender(a, b, "budget")
  let message = Message::data(Bytes::from_array(Array::make(5000, 0x41)))
  // The public value budget bounds all retained incoming encoded payloads together.
  // A private endpoint with the same negotiated state permits a compact regression.
  let limited = { ..b, limits: { ..b.limits, max_value_bytes: 8192, }, }
  ignore(a.send(handle, message, b"one", settled=true))
  limited.receive(a.drain())
  assert_true(limited.event_bytes > 5000)
  ignore(a.send(handle, message, b"two", settled=true))
  assert_true(
    try {
      limited.receive(a.drain())
      false
    } catch {
      _ => true
    },
  )
  assert_true(limited.is_failed())
  assert_eq(limited.partial_bytes, 0)
  assert_eq(limited.event_bytes, 0)
}

///|
test "negotiated sender settle mode is enforced before outbound mutation" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "settled-only")
  a.primary.local_link(handle).settle_mode = 1
  let before = a.session_positions()
  assert_true(
    try {
      ignore(a.send(handle, Message::data(b"x"), b"t"))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(a.session_positions(), before)
  assert_eq(a.link_credit(handle), 4U)
  assert_eq(a.send(handle, Message::data(b"x"), b"t", settled=true), 0U)
  b.receive(a.drain())
  assert_true(b.events()[0] is Received({ settled: true, .. }))
}

///|
test "unsettled delivery tags are unique per link and reusable after acceptance" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "unique")
  let message = Message::data(b"job")
  let id = a.send(handle, message, b"tag")
  let before = a.session_positions()
  assert_true(
    try {
      ignore(a.send(handle, message, b"tag"))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(a.send(handle, message, b"tag", settled=true))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(a.session_positions(), before)
  b.receive(a.drain())
  ignore(b.events())
  b.acknowledge(id, Accepted)
  a.receive(b.drain())
  ignore(a.events())
  assert_eq(a.send(handle, message, b"tag"), id + 1U)
}

///|
test "independent links can each have an unsettled tag with identical bytes" {
  let (a, b) = connected_pair()
  let (first, _) = ready_sender(a, b, "first", handle=41U)
  let (second, _) = ready_sender(a, b, "second", handle=42U)
  ignore(a.send(first, Message::data(b"a"), b"same"))
  ignore(a.send(second, Message::data(b"b"), b"same"))
  b.receive(a.drain())
  assert_eq(b.events().length(), 2)
  assert_eq(b.pending().1, 2)
}
