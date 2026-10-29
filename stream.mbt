///|
pub enum StreamItem {
  Header
  AmqpFrame(Frame)
}

///|
pub struct FrameStream {
  priv limits : Limits
  priv pending : Array[Byte]
  priv mut expected : Int32
  priv mut header_needed : Bool
  priv mut failed : Bool
  priv mut ended : Bool
}

///|
pub fn FrameStream::new(
  expect_header~ : Bool = true,
  limits~ : Limits = Limits::default(),
) -> FrameStream raise AmqpError {
  limits.validate()
  {
    limits,
    pending: [],
    expected: 8,
    header_needed: expect_header,
    failed: false,
    ended: false,
  }
}

///|
/// Each byte is appended once; no prefix rescanning on one-byte input feeds.
pub fn FrameStream::feed(
  self : FrameStream,
  bytes : Bytes,
) -> Array[StreamItem] raise AmqpError {
  if self.failed || self.ended {
    raise State("frame stream is closed or failed")
  }
  errdefer {
    self.failed = true
  }
  let items : Array[StreamItem] = []
  for byte in bytes {
    self.pending.push(byte)
    if self.pending.length() == 8 && !self.header_needed {
      let header = wire_reader(Bytes::from_array(self.pending), self.limits)
      let size = header.length(4)
      let offset = header.u8() * 4
      if size < 8 || offset < 8 || offset > size {
        raise Invalid("frame header size", 0)
      }
      if size > self.limits.max_frame_bytes {
        raise Limit("announced frame size")
      }
      self.expected = size
    }
    if self.pending.length() == self.expected {
      if items.length() >= self.limits.max_items {
        raise Limit("frames per feed")
      }
      let item = if self.header_needed {
        validate_protocol_header(Bytes::from_array(self.pending))
        self.header_needed = false
        Header
      } else {
        AmqpFrame(
          decode_frame(Bytes::from_array(self.pending), limits=self.limits),
        )
      }
      items.push(item)
      self.pending.clear()
      self.expected = 8
    }
  }
  items
}

///|
pub fn FrameStream::finish(self : FrameStream) -> Unit raise AmqpError {
  if self.failed || self.ended {
    raise State("frame stream is closed or failed")
  }
  self.ended = true
  if self.header_needed || !self.pending.is_empty() {
    raise Invalid("truncated AMQP stream", self.pending.length())
  }
}

///|
pub fn FrameStream::buffered_bytes(self : FrameStream) -> Int32 {
  self.pending.length()
}

///|
test "one-byte feeds reconstruct header and frames and latch malformed input" {
  let bytes = protocol_header() +
    encode_frame({
      channel: 0,
      performative: Some(Performative::new(0x10, [Text("c")])),
      payload: b"",
      extended_header: b"",
    })
  let stream = FrameStream::new()
  let mut count = 0
  for byte in bytes {
    count += stream.feed(Bytes::from_array([byte])).length()
  }
  stream.finish()
  assert_eq(count, 2)
  assert_eq(stream.buffered_bytes(), 0)
  let broken = FrameStream::new(expect_header=false)
  assert_true(
    try {
      ignore(broken.feed(b"\x00\x00\x00\x01\x02\x00\x00\x00"))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(broken.feed(b""))
      false
    } catch {
      _ => true
    },
  )
}
