///|
priv struct TestRandom {
  mut seed : UInt32
}

///|
fn TestRandom::next(self : TestRandom, bound : Int32) -> Int32 {
  self.seed = self.seed * 1664525U + 1013904223U
  (self.seed.to_uint64() % bound.to_uint64()).to_int()
}

///|
fn random_value(random : TestRandom, depth : Int32) -> Value {
  let kind = random.next(if depth > 0 { 12 } else { 8 })
  match kind {
    0 => Null
    1 => UInt(random.seed)
    2 => ULong((random.seed.to_uint64() << 32) | random.next(65536).to_uint64())
    3 => IntValue(random.seed.reinterpret_as_int())
    4 => Boolean(random.next(2) == 1)
    5 => Text("value-{random.next(10000)}-月")
    6 =>
      Binary(
        Bytes::from_array(
          Array::makei(random.next(40), _ => random.next(256).to_byte()),
        ),
      )
    7 => FloatValue(Float::reinterpret_from_uint(random.seed))
    8 =>
      List(Array::makei(random.next(4), _ => random_value(random, depth - 1)))
    9 =>
      MapValue([
        (Text("a"), random_value(random, depth - 1)),
        (Text("b"), random_value(random, depth - 1)),
      ])
    10 =>
      ArrayValue(
        0x70,
        None,
        Array::makei(random.next(5), _ => {
          UInt(random.next(1000).reinterpret_as_uint())
        }),
      )
    _ =>
      Described(
        ULong(random.next(1000).to_uint64()),
        random_value(random, depth - 1),
      )
  }
}

///|
test "deterministic generated value trees round-trip and canonicalize idempotently" {
  let random = { seed: 20261029U, }
  for i = 0; i < 1000; i = i + 1 {
    let original = encode_value(random_value(random, 4))
    let rewritten = encode_value(decode_value(original))
    assert_eq(rewritten, original)
  }
}

///|
test "bounded random bytes either fail safely or produce stable canonical values" {
  let random = { seed: 0x12345678U, }
  let limits = {
    ..Limits::default(),
    max_value_bytes: 512,
    max_items: 64,
    max_depth: 8,
  }
  let mut valid = 0
  let mut invalid = 0
  for i = 0; i < 4000; i = i + 1 {
    let bytes = Bytes::from_array(
      Array::makei(random.next(128), _ => random.next(256).to_byte()),
    )
    let result = Some(decode_value(bytes, limits~)) catch { _ => None }
    match result {
      Some(value) => {
        valid += 1
        let canonical = encode_value(value, limits~)
        assert_eq(
          encode_value(decode_value(canonical, limits~), limits~),
          canonical,
        )
      }
      None => invalid += 1
    }
  }
  assert_eq(valid + invalid, 4000)
  assert_true(invalid > 3000)
}

///|
test "every two-part split preserves a multi-frame protocol stream" {
  let bytes = protocol_header() +
    encode_frame({
      channel: 0,
      performative: Some(Performative::new(0x10, [Text("split")])),
      payload: b"",
      extended_header: b"",
    }) +
    encode_frame({
      channel: 0,
      performative: None,
      payload: b"",
      extended_header: b"",
    })
  for split = 0; split <= bytes.length(); split = split + 1 {
    let stream = FrameStream::new()
    let mut count = stream.feed(bytes[:split].to_owned()).length()
    count += stream.feed(bytes[split:].to_owned()).length()
    stream.finish()
    assert_eq(count, 3)
  }
}

///|
test "message fragmentation boundaries preserve data for several negotiated frame sizes" {
  for frame_bytes in [512, 1024, 4096] {
    for length in [0, 1, 255, 511, 512, 513, 1023, 4096, 10000] {
      let (a, b) = connected_pair(window=128U, frame_bytes~)
      let (handle, _) = ready_sender(a, b, "boundary", credit=1U)
      let data = Bytes::from_array(
        Array::makei(length, i => (i % 251).to_byte()),
      )
      ignore(a.send(handle, Message::data(data), b"unique", settled=true))
      b.receive(a.drain())
      match b.events()[0] {
        Received(delivery) => assert_eq(delivery.message.data_bytes(), data)
        _ => fail("missing delivery")
      }
    }
  }
}
