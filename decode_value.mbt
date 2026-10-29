///|
pub fn decode_value(
  bytes : Bytes,
  limits~ : Limits = Limits::default(),
) -> Value raise AmqpError {
  let reader = wire_reader(bytes, limits)
  let result = read_value(reader, 0)
  if reader.pos != reader.end {
    raise Invalid("trailing value bytes", reader.pos)
  }
  result
}

///|
fn read_value(reader : WireReader, depth : Int32) -> Value raise AmqpError {
  reader.visit(depth)
  let code = reader.u8()
  if code == 0 {
    let descriptor = read_value(reader, depth + 1)
    if !descriptor_valid(descriptor) {
      raise Invalid("descriptor type", reader.pos)
    }
    Described(descriptor, read_value(reader, depth + 1))
  } else {
    read_body(reader, code, depth)
  }
}

///|
fn signed_byte(number : Int32) -> Int32 {
  if number >= 128 {
    number - 256
  } else {
    number
  }
}

///|
fn read_body(
  reader : WireReader,
  code : Int32,
  depth : Int32,
) -> Value raise AmqpError {
  match code {
    0x40 => Null
    0x41 => Boolean(true)
    0x42 => Boolean(false)
    0x43 => UInt(0U)
    0x44 => ULong(0UL)
    0x45 => List([])
    0x50 => UByte(reader.u8())
    0x51 => ByteValue(signed_byte(reader.u8()))
    0x52 => UInt(reader.u8().reinterpret_as_uint())
    0x53 => ULong(reader.u8().to_uint64())
    0x54 => IntValue(signed_byte(reader.u8()))
    0x55 => Long(signed_byte(reader.u8()).to_int64())
    0x56 => {
      let b = reader.u8()
      if b > 1 {
        raise Invalid("boolean must be zero or one", reader.pos - 1)
      }
      Boolean(b == 1)
    }
    0x60 => UShort(reader.number(2).to_int())
    0x61 => {
      let n = reader.number(2).to_int()
      Short(if n >= 32768 { n - 65536 } else { n })
    }
    0x70 => UInt(reader.number(4).to_uint())
    0x71 => IntValue(reader.number(4).to_uint().reinterpret_as_int())
    0x72 => FloatValue(Float::reinterpret_from_uint(reader.number(4).to_uint()))
    0x73 => {
      let n = reader.number(4).to_uint()
      if n > 0x10ffffU || (n >= 0xd800U && n <= 0xdfffU) {
        raise Invalid("Unicode scalar", reader.pos - 4)
      }
      Character(n)
    }
    0x74 => Decimal32(reader.take(4))
    0x80 => ULong(reader.number(8))
    0x81 => Long(reader.number(8).reinterpret_as_int64())
    0x82 => DoubleValue(reader.number(8).reinterpret_as_double())
    0x83 => Timestamp(reader.number(8).reinterpret_as_int64())
    0x84 => Decimal64(reader.take(8))
    0x94 => Decimal128(reader.take(16))
    0x98 => Uuid(reader.take(16))
    0xa0 | 0xb0 =>
      Binary(reader.take(reader.length(if code == 0xa0 { 1 } else { 4 })))
    0xa1 | 0xb1 | 0xa3 | 0xb3 => {
      let symbol = code == 0xa3 || code == 0xb3
      let width = if code == 0xa1 || code == 0xa3 { 1 } else { 4 }
      let text = reader.text(reader.length(width), symbol)
      if symbol {
        Symbol(text)
      } else {
        Text(text)
      }
    }
    0xc0 | 0xd0 | 0xc1 | 0xd1 => read_container(reader, code, depth)
    0xe0 | 0xf0 => read_array(reader, code, depth)
    _ => raise Unsupported("unknown AMQP type code {code}")
  }
}

///|
fn container_bounds(
  reader : WireReader,
  width : Int32,
) -> (Int32, Int32) raise AmqpError {
  let size = reader.length(width)
  if size < width || size > reader.end - reader.pos {
    raise Invalid("container byte size", reader.pos)
  }
  let end = reader.pos + size
  let count = reader.length(width)
  if count > reader.limits.max_items - reader.nodes {
    raise Limit("container item count")
  }
  (end, count)
}

///|
fn read_container(
  reader : WireReader,
  code : Int32,
  depth : Int32,
) -> Value raise AmqpError {
  let (end, count) = container_bounds(
    reader,
    if code == 0xc0 || code == 0xc1 {
      1
    } else {
      4
    },
  )
  if count > end - reader.pos {
    raise Invalid("container count exceeds bytes", reader.pos)
  }
  let map = code == 0xc1 || code == 0xd1
  if map && count % 2 != 0 {
    raise Invalid("map requires even count", reader.pos)
  }
  let items : Array[Value] = []
  for i = 0; i < count; i = i + 1 {
    items.push(read_value(reader, depth + 1))
  }
  if reader.pos != end {
    raise Invalid("container size/count mismatch", reader.pos)
  }
  if map {
    let pairs : Array[(Value, Value)] = []
    let seen : Map[Bytes, Bool] = Map([])
    for i = 0; i < count; i = i + 2 {
      let key = encode_value(items[i], limits=reader.limits)
      if seen.contains(key) {
        raise Invalid("duplicate map key value", reader.pos)
      }
      seen[key] = true
      pairs.push((items[i], items[i + 1]))
    }
    MapValue(pairs)
  } else {
    List(items)
  }
}

///|
fn read_array(
  reader : WireReader,
  code : Int32,
  depth : Int32,
) -> Value raise AmqpError {
  let (end, count) = container_bounds(reader, if code == 0xe0 { 1 } else { 4 })
  let first = reader.u8()
  let (descriptor, element_code) = if first == 0 {
    let descriptor = read_value(reader, depth + 1)
    if !descriptor_valid(descriptor) {
      raise Invalid("array descriptor type", reader.pos)
    }
    (Some(descriptor), reader.u8())
  } else {
    (None, first)
  }
  if !valid_array_code(element_code) {
    raise Invalid("array constructor", reader.pos - 1)
  }
  let items : Array[Value] = []
  for i = 0; i < count; i = i + 1 {
    reader.visit(depth + 1)
    items.push(read_body(reader, element_code, depth + 1))
  }
  if reader.pos != end {
    raise Invalid("array size/count mismatch", reader.pos)
  }
  let normalized = if items.is_empty() {
    normalize_code(element_code)
  } else {
    value_code(items[0])
  }
  ArrayValue(normalized, descriptor, items)
}

///|
fn valid_array_code(code : Int32) -> Bool {
  canonical_array_code(normalize_code(code))
}

///|
fn normalize_code(code : Int32) -> Int32 {
  match code {
    0x41 | 0x42 => 0x56
    0x43 | 0x52 => 0x70
    0x44 | 0x53 => 0x80
    0x54 => 0x71
    0x55 => 0x81
    0x45 | 0xc0 => 0xd0
    0xc1 => 0xd1
    0xa0 => 0xb0
    0xa1 => 0xb1
    0xa3 => 0xb3
    0xe0 => 0xf0
    _ => code
  }
}

///|
test "compact scalar forms normalize without losing wire type" {
  assert_eq(encode_value(decode_value(b"\x53\xff")), encode_value(ULong(255UL)))
  assert_eq(encode_value(decode_value(b"\x54\xff")), encode_value(IntValue(-1)))
  assert_eq(encode_value(decode_value(b"\x41")), encode_value(Boolean(true)))
  assert_true(
    try {
      ignore(decode_value(b"\x56\x02"))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(decode_value(b"\xc1\x02\x01\x40"))
      false
    } catch {
      _ => true
    },
  )
}
