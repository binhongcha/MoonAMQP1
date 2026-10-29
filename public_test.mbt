///|
test "consumer builds messages and observes exact typed application properties" {
  let message = @moonamqp1.Message::data(b"payload", properties=[
    ("sequence", ULong(18446744073709551615UL)),
  ])
  let decoded = @moonamqp1.decode_message(@moonamqp1.encode_message(message))
  assert_eq(decoded.data_bytes(), b"payload")
  let sections = decoded.sections()
  match sections[0].body {
    MapValue(entries) =>
      assert_eq(
        @moonamqp1.encode_value(entries[0].1),
        b"\x80\xff\xff\xff\xff\xff\xff\xff\xff",
      )
    _ => fail("application properties required")
  }
}

///|
test "consumer accessors cannot mutate retained message or performative lists" {
  let fields = [@moonamqp1.Value::Text("original")]
  let perf = @moonamqp1.Performative::new(0x10, fields)
  fields[0] = Text("changed")
  assert_eq(perf.field(0).as_text(), "original")
  let items = [@moonamqp1.Value::Text("a")]
  let message = @moonamqp1.Message::new([
    { descriptor: 0x77, body: List(items), },
  ])
  items[0] = Text("caller changed")
  let first = message.sections()
  match first[0].body {
    List(fields) => fields[0] = Text("accessor changed")
    _ => fail("list body")
  }
  match message.sections()[0].body {
    List(fields) => assert_eq(fields[0].as_text(), "a")
    _ => fail("list body")
  }
}

///|
test "consumer can create symbol arrays and exact negative timestamp domains" {
  let array = @moonamqp1.Value::ArrayValue(0xb3, None, [
    Symbol("a"),
    Symbol("b"),
  ])
  let bytes = @moonamqp1.encode_value(array)
  assert_eq(@moonamqp1.encode_value(@moonamqp1.decode_value(bytes)), bytes)
  let timestamp = @moonamqp1.encode_value(Timestamp(-1L))
  assert_eq(timestamp, b"\x83\xff\xff\xff\xff\xff\xff\xff\xff")
}

///|
test "consumer sees the difference between absent null and empty data bodies" {
  let absent = @moonamqp1.Message::new([])
  let null_body = @moonamqp1.Message::new([{ descriptor: 0x77, body: Null, }])
  let empty_data = @moonamqp1.Message::data(b"")
  assert_eq(@moonamqp1.encode_message(absent), b"")
  assert_true(
    @moonamqp1.encode_message(null_body) !=
    @moonamqp1.encode_message(empty_data),
  )
  assert_true(
    try {
      ignore(null_body.data_bytes())
      false
    } catch {
      _ => true
    },
  )
}

///|
test "consumer extracts structured failures rather than process panics" {
  let error = try {
    ignore(@moonamqp1.decode_value(b"\x70\x00"))
    "no error"
  } catch {
    @moonamqp1.AmqpError::Invalid(_, _) => "invalid"
    _ => "other"
  }
  assert_eq(error, "invalid")
  assert_true(
    try {
      ignore(@moonamqp1.Endpoint::new(""))
      false
    } catch {
      _ => true
    },
  )
}

///|
test "consumer can encode and incrementally decode an extended-header transfer frame" {
  let transfer = @moonamqp1.Performative::new(0x14, [
    UInt(17U),
    UInt(0U),
    Binary(b"tag"),
  ])
  let frame : @moonamqp1.Frame = {
    channel: 23,
    performative: Some(transfer),
    payload: b"message",
    extended_header: b"\x00\x00\x00\x00",
  }
  let bytes = @moonamqp1.encode_frame(frame)
  let stream = @moonamqp1.FrameStream::new(expect_header=false)
  let mut seen = false
  for byte in bytes {
    for item in stream.feed(Bytes::from_array([byte])) {
      match item {
        AmqpFrame(frame) => {
          assert_eq(frame.channel, 23)
          assert_eq(frame.payload, b"message")
          seen = true
        }
        Header => fail("unexpected header")
      }
    }
  }
  stream.finish()
  assert_true(seen)
}
