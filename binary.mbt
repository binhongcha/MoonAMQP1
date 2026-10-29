///|
priv struct WireReader {
  data : Bytes
  mut pos : Int32
  end : Int32
  limits : Limits
  mut nodes : Int32
}

///|
fn wire_reader(data : Bytes, limits : Limits) -> WireReader raise AmqpError {
  limits.validate()
  if data.length() > limits.max_value_bytes {
    raise Limit("wire input bytes")
  }
  { data, pos: 0, end: data.length(), limits, nodes: 0, }
}

///|
fn WireReader::u8(self : WireReader) -> Int32 raise AmqpError {
  if self.pos >= self.end {
    raise Invalid("truncated byte", self.pos)
  }
  let value = self.data[self.pos].to_int()
  self.pos += 1
  value
}

///|
fn WireReader::take(self : WireReader, size : Int32) -> Bytes raise AmqpError {
  if size < 0 || size > self.end - self.pos {
    raise Invalid("truncated bytes", self.pos)
  }
  let result = self.data[self.pos:self.pos + size].to_owned()
  self.pos += size
  result
}

///|
fn WireReader::number(
  self : WireReader,
  size : Int32,
) -> UInt64 raise AmqpError {
  if size < 0 || size > 8 {
    raise Invalid("integer width", self.pos)
  }
  let mut result = 0UL
  for i = 0; i < size; i = i + 1 {
    result = (result << 8) | self.u8().to_uint64()
  }
  result
}

///|
fn WireReader::length(
  self : WireReader,
  width : Int32,
) -> Int32 raise AmqpError {
  let value = self.number(width)
  if value > 2147483647UL {
    raise Limit("wire length")
  }
  value.to_int()
}

///|
fn WireReader::visit(self : WireReader, depth : Int32) -> Unit raise AmqpError {
  if depth > self.limits.max_depth || self.nodes >= self.limits.max_items {
    raise Limit("value nesting or node budget")
  }
  self.nodes += 1
}

///|
priv struct WireWriter {
  bytes : Array[Byte]
  maximum : Int32
}

///|
fn wire_writer(maximum : Int32) -> WireWriter {
  { bytes: [], maximum, }
}

///|
fn WireWriter::u8(self : WireWriter, value : Int32) -> Unit raise AmqpError {
  if self.bytes.length() >= self.maximum {
    raise Limit("encoded bytes")
  }
  self.bytes.push(value.to_byte())
}

///|
fn WireWriter::number(
  self : WireWriter,
  value : UInt64,
  width : Int32,
) -> Unit raise AmqpError {
  for i = width - 1; i >= 0; i = i - 1 {
    self.u8(((value >> (i * 8)) & 255UL).to_int())
  }
}

///|
fn WireWriter::put(self : WireWriter, bytes : Bytes) -> Unit raise AmqpError {
  if bytes.length() > self.maximum - self.bytes.length() {
    raise Limit("encoded bytes")
  }
  for byte in bytes {
    self.bytes.push(byte)
  }
}

///|
fn WireWriter::finish(self : WireWriter) -> Bytes {
  Bytes::from_array(self.bytes)
}

///|
fn text_bytes(
  value : String,
  maximum~ : Int32 = 2147483647,
) -> Bytes raise AmqpError {
  let units = value.code_units()
  let mut index = 0
  let mut size = 0
  while index < units.length() {
    let unit = units[index].to_int()
    if unit >= 0xd800 && unit <= 0xdbff {
      if index + 1 >= units.length() ||
        units[index + 1].to_int() < 0xdc00 ||
        units[index + 1].to_int() > 0xdfff {
        raise Invalid("unpaired UTF-16 leading surrogate", index)
      }
      size = checked_add(size, 4)
      index += 2
    } else {
      if unit >= 0xdc00 && unit <= 0xdfff {
        raise Invalid("unpaired UTF-16 trailing surrogate", index)
      }
      size = checked_add(
        size,
        if unit < 0x80 {
          1
        } else if unit < 0x800 {
          2
        } else {
          3
        },
      )
      index += 1
    }
    if size > maximum {
      raise Limit("UTF-8 encoded bytes")
    }
  }
  @utf8.encode(value)
}

///|
fn WireReader::text(
  self : WireReader,
  size : Int32,
  ascii : Bool,
) -> String raise AmqpError {
  let start = self.pos
  let bytes = self.take(size)
  if ascii {
    for byte in bytes {
      if byte.to_int() > 127 {
        raise Invalid("non-ASCII symbol", start)
      }
    }
  }
  @utf8.decode(bytes[:]) catch {
    _ => raise Invalid("invalid UTF-8", start)
  }
}

///|
test "network integers are big endian and truncated input is an error" {
  let writer = wire_writer(20)
  writer.number(0x12345678UL, 4)
  assert_eq(writer.finish(), b"\x12\x34\x56\x78")
  let reader = wire_reader(writer.finish(), Limits::default())
  assert_eq(reader.number(4), 0x12345678UL)
  assert_true(
    try {
      ignore(reader.u8())
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(checked_add(2147483647, 1))
      false
    } catch {
      _ => true
    },
  )
}
