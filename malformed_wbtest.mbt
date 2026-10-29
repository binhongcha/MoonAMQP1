///|
fn peer_wire(
  b : Endpoint,
  code : Int32,
  fields : Array[Value],
  payload~ : Bytes = b"",
) -> Bytes raise AmqpError {
  b.frame(code, fields, payload~)
}

///|
test "all incomplete frame prefixes fail at EOF including missing protocol header" {
  let bytes = protocol_header() +
    encode_frame({
      channel: 0,
      performative: Some(Performative::new(0x10, [Text("peer")])),
      payload: b"",
      extended_header: b"",
    })
  for length = 0; length < bytes.length(); length = length + 1 {
    // A standalone complete header is a valid stream item but not a connection.
    if length == 8 {
      continue
    }
    let stream = FrameStream::new()
    ignore(stream.feed(bytes[:length].to_owned()))
    assert_true(
      try {
        stream.finish()
        false
      } catch {
        _ => true
      },
    )
  }
}

///|
test "oversize frames are rejected after eight bytes without buffering their bodies" {
  let stream = FrameStream::new(expect_header=false, limits={
    ..Limits::default(),
    max_frame_bytes: 512,
  })
  assert_true(
    try {
      ignore(stream.feed(b"\x00\x10\x00\x00\x02\x00\x00\x00"))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(stream.buffered_bytes(), 8)
  assert_true(
    try {
      validate_protocol_header(b"AMQP\x03\x01\x00\x00")
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(decode_frame(b"\x00\x00\x00\x08\x02\x01\x00\x00"))
      false
    } catch {
      _ => true
    },
  )
}

///|
test "nontransfer payload unsupported fields and malformed field types are rejected" {
  let open = Performative::new(0x10, [Text("peer")])
  assert_true(
    try {
      ignore(
        encode_frame({
          channel: 0,
          performative: Some(open),
          payload: b"garbage",
          extended_header: b"",
        }),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(Performative::new(0x11, [Null, ULong(0UL), UInt(1U), UInt(1U)]))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(Performative::new(0x10, [Text("x"), Null, UInt(511U)]))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(Performative::new(0x17, [Null, Null]))
      false
    } catch {
      _ => true
    },
  )
}

///|
test "wire failures latch endpoint state and reject all later commands" {
  let endpoint = Endpoint::new("e")
  assert_true(
    try {
      endpoint.receive(b"AMQP\x03\x01\x00\x00")
      false
    } catch {
      _ => true
    },
  )
  assert_true(endpoint.is_failed())
  assert_true(
    try {
      endpoint.start()
      false
    } catch {
      _ => true
    },
  )
  assert_eq(endpoint.drain(), b"")
}

///|
test "unknown channel handle and role do not enter valid receive states" {
  let (a, b) = connected_pair()
  let bad = encode_frame({
    channel: 999,
    performative: Some(Performative::new(0x16, [UInt(2U), Boolean(true)])),
    payload: b"",
    extended_header: b"",
  })
  assert_true(
    try {
      a.receive(bad)
      false
    } catch {
      _ => true
    },
  )
  assert_true(a.is_failed())
  let (c, d) = connected_pair()
  let perf = Performative::new(0x14, [UInt(999U), UInt(0U), Binary(b"t")])
  assert_true(
    try {
      c.receive(d.frame(0x14, perf.fields, payload=b""))
      false
    } catch {
      _ => true
    },
  )
  ignore(b)
}

///|
test "remote sender cannot start a delivery without granted link credit" {
  let (a, b) = connected_pair()
  let (_, receiver) = ready_sender(a, b, "no-credit", credit=0U)
  let remote = b.local_link(receiver).remote_handle.unwrap()
  let bytes = peer_wire(
    a,
    0x14,
    [UInt(remote), UInt(0U), Binary(b"t"), UInt(0U)],
    payload=encode_message(Message::data(b"x")),
  )
  assert_true(
    try {
      b.receive(bytes)
      false
    } catch {
      _ => true
    },
  )
  assert_true(b.is_failed())
}

///|
test "aborted delivery releases fragment memory and emits no received message" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "abort")
  let first = peer_wire(
    a,
    0x14,
    [
      UInt(handle),
      UInt(0U),
      Binary(b"t"),
      UInt(0U),
      Boolean(false),
      Boolean(true),
    ],
    payload=b"incomplete",
  )
  b.receive(first)
  assert_eq(b.partial_bytes, 10)
  let abort_frame = peer_wire(a, 0x14, [
    UInt(handle),
    Null,
    Null,
    Null,
    Null,
    Boolean(false),
    Null,
    Null,
    Null,
    Boolean(true),
  ])
  b.receive(abort_frame)
  assert_true(b.events()[0] is Aborted(_, 0U))
  assert_eq(b.partial_bytes, 0)
  assert_eq(b.pending(), (0, 0))
}

///|
test "continuation identity mismatch rejects incomplete message and releases memory" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "mismatch")
  b.receive(
    peer_wire(
      a,
      0x14,
      [
        UInt(handle),
        UInt(0U),
        Binary(b"a"),
        UInt(0U),
        Boolean(false),
        Boolean(true),
      ],
      payload=b"part",
    ),
  )
  assert_true(
    try {
      b.receive(
        peer_wire(
          a,
          0x14,
          [UInt(handle), UInt(1U), Binary(b"b")],
          payload=b"tail",
        ),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_eq(b.partial_bytes, 0)
}

///|
test "huge disposition range scans only bounded known deliveries" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "ranges")
  ignore(a.send(handle, Message::data(b"one"), b"1"))
  ignore(a.send(handle, Message::data(b"two"), b"2"))
  ignore(a.drain())
  a.receive(
    peer_wire(b, 0x15, [
      Boolean(true),
      UInt(0U),
      UInt(2147483646U),
      Boolean(true),
      Outcome::Released.value(),
    ]),
  )
  assert_eq(a.pending().0, 0)
  assert_eq(a.events().length(), 2)
}

///|
test "fragment count budget stops many zero-length continuations" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "fragments")
  let limited = { ..b, limits: { ..b.limits, max_fragments: 2, }, }
  limited.receive(
    peer_wire(a, 0x14, [
      UInt(handle),
      UInt(0U),
      Binary(b"t"),
      UInt(0U),
      Boolean(false),
      Boolean(true),
    ]),
  )
  let more = peer_wire(a, 0x14, [
    UInt(handle),
    Null,
    Null,
    Null,
    Null,
    Boolean(true),
  ])
  limited.receive(more)
  assert_true(
    try {
      limited.receive(more)
      false
    } catch {
      _ => true
    },
  )
  assert_true(limited.is_failed())
}

///|
test "unsupported resume transactions and receiver-second fail explicitly" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "resume")
  assert_true(
    try {
      b.receive(
        peer_wire(a, 0x14, [
          UInt(handle),
          UInt(0U),
          Binary(b"t"),
          UInt(0U),
          Null,
          Null,
          Null,
          Null,
          Boolean(true),
        ]),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(Outcome::from_value(Described(ULong(0x33UL), List([]))))
      false
    } catch {
      _ => true
    },
  )
  let link = link_state("second", 0U, true, "q")
  assert_true(
    try {
      link.attach(
        Performative::new(0x12, [
          Text("second"),
          UInt(1U),
          Boolean(true),
          UByte(2),
          UByte(1),
        ]),
      )
      false
    } catch {
      _ => true
    },
  )
}

///|
test "receiver rejects tag reuse while prior delivery remains unsettled" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "duplicate-tag")
  let payload = encode_message(Message::data(b"job"))
  b.receive(
    peer_wire(
      a,
      0x14,
      [UInt(handle), UInt(0U), Binary(b"same"), UInt(0U)],
      payload~,
    ),
  )
  ignore(b.events())
  assert_true(
    try {
      b.receive(
        peer_wire(
          a,
          0x14,
          [UInt(handle), UInt(1U), Binary(b"same"), UInt(0U)],
          payload~,
        ),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(b.is_failed())
}

///|
test "incomplete deliveries consume pending capacity and cannot be acknowledged early" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "partial-pending")
  b.receive(
    peer_wire(
      a,
      0x14,
      [
        UInt(handle),
        UInt(0U),
        Binary(b"part"),
        UInt(0U),
        Boolean(false),
        Boolean(true),
      ],
      payload=b"partial",
    ),
  )
  assert_eq(b.pending().1, 1)
  assert_true(
    try {
      b.acknowledge(0U, Accepted)
      false
    } catch {
      _ => true
    },
  )
  assert_eq(b.pending().1, 1)
  b.receive(
    peer_wire(a, 0x14, [
      UInt(handle),
      Null,
      Null,
      Null,
      Null,
      Boolean(false),
      Null,
      Null,
      Null,
      Boolean(true),
    ]),
  )
  assert_eq(b.pending().1, 0)
}

///|
test "wrapped delivery IDs cannot alias an old pending delivery even if pre-settled" {
  let (a, b) = connected_pair()
  let (handle, _) = ready_sender(a, b, "wrap-collision")
  let payload = encode_message(Message::data(b"job"))
  b.receive(
    peer_wire(
      a,
      0x14,
      [UInt(handle), UInt(0U), Binary(b"old"), UInt(0U)],
      payload~,
    ),
  )
  ignore(b.events())
  // Model a full serial cycle while the original unsettled ID survives.
  b.remote_next_delivery = Some(0U)
  assert_true(
    try {
      b.receive(
        peer_wire(
          a,
          0x14,
          [UInt(handle), UInt(0U), Binary(b"new"), UInt(0U), Boolean(true)],
          payload~,
        ),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(b.is_failed())
}
