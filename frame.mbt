///|
pub(all) struct Frame {
  channel : Int32
  performative : Performative?
  payload : Bytes
  extended_header : Bytes
}

///|
pub fn protocol_header() -> Bytes {
  b"AMQP\x00\x01\x00\x00"
}

///|
pub fn validate_protocol_header(bytes : Bytes) -> Unit raise AmqpError {
  if bytes != protocol_header() {
    raise Unsupported(
      "expected AMQP 1.0 protocol header; SASL/TLS are host responsibilities",
    )
  }
}

///|
pub fn encode_frame(
  frame : Frame,
  limits~ : Limits = Limits::default(),
) -> Bytes raise AmqpError {
  limits.validate()
  require_range(frame.channel, 0, 65535)
  if frame.extended_header.length() % 4 != 0 ||
    frame.extended_header.length() > 1012 {
    raise Invalid("extended frame header alignment", 0)
  }
  let body = match frame.performative {
    Some(perf) => {
      if perf.code != 0x14 && frame.payload.length() != 0 {
        raise Invalid("payload on non-transfer frame", 0)
      }
      encode_value(perf.value(), limits~)
    }
    None => {
      if frame.payload.length() != 0 || frame.channel != 0 {
        raise Invalid("heartbeat frame", 0)
      }
      b""
    }
  }
  let header_size = checked_add(8, frame.extended_header.length())
  let total = checked_add(
    checked_add(header_size, body.length()),
    frame.payload.length(),
  )
  if total > limits.max_frame_bytes {
    raise Limit("frame size")
  }
  let writer = wire_writer(total)
  writer.number(total.to_uint64(), 4)
  writer.u8(header_size / 4)
  writer.u8(0)
  writer.number(frame.channel.to_uint64(), 2)
  writer.put(frame.extended_header)
  writer.put(body)
  writer.put(frame.payload)
  writer.finish()
}

///|
pub fn decode_frame(
  bytes : Bytes,
  limits~ : Limits = Limits::default(),
) -> Frame raise AmqpError {
  limits.validate()
  if bytes.length() > limits.max_frame_bytes {
    raise Limit("frame size")
  }
  let reader = wire_reader(bytes, limits)
  let size = reader.length(4)
  let offset = reader.u8() * 4
  let kind = reader.u8()
  let channel = reader.number(2).to_int()
  if size != bytes.length() || size < 8 || offset < 8 || offset > size {
    raise Invalid("frame size or data offset", 0)
  }
  if kind != 0 {
    raise Unsupported("non-AMQP frame type")
  }
  let extended_header = reader.take(offset - 8)
  let performative = if reader.pos == size {
    if channel != 0 {
      raise Invalid("heartbeat channel must be zero", 6)
    }
    None
  } else {
    Some(Performative::from_value(read_value(reader, 0), limits~))
  }
  let payload = reader.take(size - reader.pos)
  match performative {
    Some(perf) if perf.code != 0x14 && payload.length() != 0 =>
      raise Invalid("unexpected frame payload", reader.pos)
    _ => ()
  }
  { channel, performative, payload, extended_header, }
}

///|
test "frame sizes offsets heartbeat and payload restrictions" {
  let frame = {
    channel: 7,
    performative: Some(Performative::new(0x10, [Text("peer")])),
    payload: b"",
    extended_header: b"abcd",
  }
  let bytes = encode_frame(frame)
  let decoded = decode_frame(bytes)
  assert_eq(decoded.channel, 7)
  assert_eq(decoded.extended_header, b"abcd")
  let heart = {
    channel: 0,
    performative: None,
    payload: b"",
    extended_header: b"",
  }
  assert_eq(encode_frame(heart), b"\x00\x00\x00\x08\x02\x00\x00\x00")
  assert_true(
    try {
      ignore(decode_frame(b"\x00\x00\x00\x08\x03\x00\x00\x00"))
      false
    } catch {
      _ => true
    },
  )
}
