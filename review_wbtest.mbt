///|
test "crossed Begin creates two sessions with independent delivery IDs and windows" {
  let a = Endpoint::new("cross-a", channel=3)
  let b = Endpoint::new("cross-b", channel=7)
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  ignore(a.events())
  ignore(b.events())
  a.begin()
  b.begin()
  let from_a = a.drain()
  a.receive(b.drain())
  b.receive(from_a)
  let from_a = a.drain()
  a.receive(b.drain())
  b.receive(from_a)
  assert_eq(a.session_diagnostics().map(s => (s.channel, s.remote_channel)), [
    (0, Some(7)),
    (3, Some(0)),
  ])
  assert_eq(b.session_diagnostics().map(s => (s.channel, s.remote_channel)), [
    (0, Some(3)),
    (7, Some(0)),
  ])
  let h = a.attach("first-channel", true, "q", handle=0U)
  let other = a.attach("second-channel", true, "q", handle=0U, channel=0)
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(0U, 1U, channel=0)
  b.grant(0U, 2U)
  a.receive(b.drain())
  ignore(a.events())
  ignore(b.events())
  assert_eq(a.send(h, Message::data(b"primary"), b"tag"), 0U)
  assert_eq(a.send(other, Message::data(b"secondary"), b"tag", channel=0), 0U)
  b.receive(a.drain())
  assert_true(b.events()[0] is SessionEvent(0, Received(_)))
  b.acknowledge(0U, Accepted, channel=0)
  a.receive(b.drain())
  assert_eq(a.pending(), (0, 0))
  let snapshot = a.session_diagnostics()
  assert_eq(snapshot[0].outgoing_unsettled, 1)
  assert_eq(snapshot[0].next_outgoing, 1U)
  assert_eq(snapshot[0].links[0].credit, 1U)
  assert_eq(snapshot[1].outgoing_unsettled, 0)
  a.end_session(channel=0)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(a.session_diagnostics().length(), 1)
  assert_eq(a.session_diagnostics()[0].channel, 3)
  assert_eq(snapshot[0].outgoing_unsettled, 1)
  a.begin(channel=0)
  b.receive(a.drain())
  a.receive(b.drain())
  assert_eq(a.session_diagnostics()[0].next_outgoing, 0U)
}

///|
test "duplicate local channel and session budget preserve the active session" {
  let a = Endpoint::new("bounded", max_sessions=1)
  let b = Endpoint::new("peer")
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  let duplicate = Ok(a.begin()) catch { e => Err(e) }
  let excessive = Ok(a.begin(channel=1)) catch { e => Err(e) }
  assert_true(duplicate is Err(State(_)))
  assert_true(excessive is Err(Limit(_)))
  assert_eq(a.drain(), b"")
  assert_eq(a.session_diagnostics().length(), 1)
}

///|
test "receiver second retains incoming records until sender confirmation" {
  let (a, b) = connected_pair()
  let receiver = b.attach("second", false, "q", receiver_mode=Second)
  a.receive(b.drain())
  b.receive(a.drain())
  let sender = a.primary.links[0].local_handle
  b.grant(receiver, 3U)
  a.receive(b.drain())
  ignore(a.events())
  for i in 0..<3 {
    ignore(
      a.send(sender, Message::data(b"item"), Bytes::from_array([i.to_byte()])),
    )
  }
  b.receive(a.drain())
  b.acknowledge_range(0U, 2U, Accepted)
  assert_eq(b.pending(), (0, 3))
  assert_eq(b.session_diagnostics()[0].awaiting_sender_confirmation, 3)
  assert_true(
    (Ok(b.acknowledge(1U, Released)) catch { e => Err(e) }) is Err(State(_)),
  )
  assert_eq(b.outbound.length(), 1)
  a.receive(b.drain())
  assert_eq(a.outbound.length(), 1)
  assert_eq(a.events().length(), 3)
  assert_eq(a.pending(), (0, 0))
  ignore(b.events())
  b.receive(a.drain())
  assert_eq(b.pending(), (0, 0))
  assert_eq(b.session_diagnostics()[0].awaiting_sender_confirmation, 0)
  assert_true(b.events()[2] is PeerSettled(2U))
}

///|
test "acknowledgement range rejects unknown IDs and rolls back on a full output budget" {
  let (a, b) = connected_pair()
  let (sender, _) = ready_sender(a, b, "atomic-range")
  ignore(a.send(sender, Message::data(b"job"), b"one"))
  b.receive(a.drain())
  assert_true(
    (Ok(b.acknowledge_range(0U, 1U, Accepted)) catch { e => Err(e) })
    is Err(State(_)),
  )
  assert_true(
    (Ok(b.acknowledge_range(0U, 0x80000000U, Accepted)) catch { e => Err(e) })
    is Err(Invalid(_, _)),
  )
  b.queued_bytes = b.limits.max_value_bytes
  assert_true(
    (Ok(b.acknowledge(0U, Accepted)) catch { e => Err(e) }) is Err(Limit(_)),
  )
  assert_eq(b.pending(), (0, 1))
  b.queued_bytes = 0
  b.acknowledge(0U, Accepted)
  assert_eq(b.pending(), (0, 0))
  assert_eq(b.outbound.length(), 1)
}

///|
test "range acknowledgement wraps sequence IDs and rejects mixed settlement modes" {
  let (a, b) = connected_pair()
  let (first, _) = ready_sender(a, b, "first-mode")
  let second = a.attach("second-mode", true, "q", receiver_mode=Second)
  b.receive(a.drain())
  a.receive(b.drain())
  b.grant(1U, 2U)
  a.receive(b.drain())
  a.primary.next_delivery = 4294967295U
  assert_eq(a.send(first, Message::data(b"first"), b"a"), 4294967295U)
  assert_eq(a.send(second, Message::data(b"second"), b"b"), 0U)
  b.receive(a.drain())
  assert_true(
    (Ok(b.acknowledge_range(4294967295U, 0U, Released)) catch { e => Err(e) })
    is Err(State(_)),
  )
  assert_eq(b.pending(), (0, 2))
  assert_eq(b.drain(), b"")
  b.acknowledge(4294967295U, Accepted)
  b.acknowledge(0U, Released)
  a.receive(b.drain())
  b.receive(a.drain())
  assert_eq(a.pending(), (0, 0))
  assert_eq(b.pending(), (0, 0))
}

///|
test "receiver-first range acknowledgement emits one disposition across uint32 wrap" {
  let (a, b) = connected_pair()
  let (sender, receiver) = ready_sender(a, b, "wrapped-range")
  b.grant(receiver, 2U)
  a.receive(b.drain())
  a.primary.next_delivery = 4294967295U
  ignore(a.send(sender, Message::data(b"first"), b"first"))
  ignore(a.send(sender, Message::data(b"last"), b"last"))
  b.receive(a.drain())
  b.acknowledge_range(4294967295U, 0U, Accepted)
  assert_eq(b.outbound.length(), 1)
  a.receive(b.drain())
  assert_eq(a.pending(), (0, 0))
  assert_eq(b.pending(), (0, 0))
}

///|
test "drain rejects a peer returning more credit than was granted" {
  let (a, b) = connected_pair()
  let (sender, receiver) = ready_sender(a, b, "malformed-drain")
  b.grant(receiver, 2U, drain=true)
  a.receive(b.drain())
  let wire = peer_wire(a, 0x13, [
    UInt(0U),
    UInt(16U),
    UInt(0U),
    UInt(16U),
    UInt(sender),
    UInt(3U),
    UInt(0U),
    Null,
    Boolean(true),
  ])
  assert_true((Ok(b.receive(wire)) catch { e => Err(e) }) is Err(Invalid(_, _)))
  assert_true(b.is_failed())
  assert_true(b.session_diagnostics().is_empty())
}

///|
test "empty drain wraps delivery-count without moving transfer or delivery IDs" {
  let (a, b) = connected_pair()
  let (sender, receiver) = ready_sender(a, b, "wrap-drain")
  a.primary.local_link(sender).delivery_count = 4294967294U
  b.primary.local_link(receiver).delivery_count = 4294967294U
  let before = a.session_positions()
  b.grant(receiver, 3U, drain=true)
  assert_true(
    (Ok(b.grant(receiver, 1U)) catch { e => Err(e) }) is Err(State(_)),
  )
  a.receive(b.drain())
  assert_true(a.events().any(event => event is DrainRequested(_, 3U)))
  a.complete_drain(sender)
  assert_true(
    (Ok(a.complete_drain(sender)) catch { e => Err(e) }) is Err(State(_)),
  )
  b.receive(a.drain())
  assert_true(b.events().any(event => event is DrainCompleted(_, 3U)))
  assert_eq(b.link_credit(receiver), 0U)
  assert_eq(b.primary.local_link(receiver).delivery_count, 1U)
  assert_eq(a.session_positions(), before)
  assert_eq(a.primary.next_delivery, 0U)
}

///|
test "drain completion waits for a fragmented delivery even when Flow arrives first" {
  let (a, b) = connected_pair()
  let (sender, receiver) = ready_sender(a, b, "fragment-drain")
  b.grant(receiver, 3U, drain=true)
  a.receive(b.drain())
  let message = encode_message(Message::data(b"split"))
  b.receive(
    peer_wire(
      a,
      0x14,
      [UInt(sender), UInt(0U), Binary(b"t"), UInt(0U), Null, Boolean(true)],
      payload=message[:4].to_owned(),
    ),
  )
  b.receive(
    peer_wire(a, 0x13, [
      UInt(0U),
      UInt(16U),
      UInt(1U),
      UInt(16U),
      UInt(sender),
      UInt(3U),
      UInt(0U),
      Null,
      Boolean(true),
    ]),
  )
  assert_true(b.events().is_empty())
  b.receive(peer_wire(a, 0x14, [UInt(sender)], payload=message[4:].to_owned()))
  let events = b.events()
  assert_true(events[0] is Received(_))
  assert_true(events[1] is DrainCompleted(_, 2U))
}
