///|
priv struct EncodeBudget {
  limits : Limits
  mut nodes : Int32
}

///|
fn EncodeBudget::visit(
  self : EncodeBudget,
  depth : Int32,
) -> Unit raise AmqpError {
  if depth > self.limits.max_depth || self.nodes >= self.limits.max_items {
    raise Limit("encoded value nesting or nodes")
  }
  self.nodes += 1
}

///|
/// Canonical full-width forms simplify deterministic output and homogeneous arrays.
pub fn encode_value(
  value : Value,
  limits~ : Limits = Limits::default(),
) -> Bytes raise AmqpError {
  limits.validate()
  let writer = wire_writer(limits.max_value_bytes)
  write_value(writer, value, { limits, nodes: 0, }, 0)
  writer.finish()
}

///|
fn write_value(
  writer : WireWriter,
  value : Value,
  budget : EncodeBudget,
  depth : Int32,
) -> Unit raise AmqpError {
  budget.visit(depth)
  match value {
    Described(descriptor, body) => {
      if !descriptor_valid(descriptor) {
        raise Invalid("descriptor must be ulong or symbol", 0)
      }
      writer.u8(0)
      write_value(writer, descriptor, budget, depth + 1)
      write_value(writer, body, budget, depth + 1)
    }
    _ => {
      let code = value_code(value)
      writer.u8(code)
      write_body(writer, code, value, budget, depth)
    }
  }
}

///|
fn require_range(
  value : Int32,
  low : Int32,
  high : Int32,
) -> Unit raise AmqpError {
  if value < low || value > high {
    raise Invalid("integer outside wire type", 0)
  }
}

///|
fn fixed_bytes(
  writer : WireWriter,
  data : Bytes,
  size : Int32,
) -> Unit raise AmqpError {
  if data.length() != size {
    raise Invalid("fixed binary width", 0)
  }
  writer.put(data)
}

///|
fn write_body(
  writer : WireWriter,
  code : Int32,
  value : Value,
  budget : EncodeBudget,
  depth : Int32,
) -> Unit raise AmqpError {
  if code != value_code(value) {
    raise Invalid("heterogeneous array element", 0)
  }
  match value {
    Null => ()
    Boolean(flag) => writer.u8(if flag { 1 } else { 0 })
    UByte(n) => {
      require_range(n, 0, 255)
      writer.u8(n)
    }
    UShort(n) => {
      require_range(n, 0, 65535)
      writer.number(n.to_uint64(), 2)
    }
    ByteValue(n) => {
      require_range(n, -128, 127)
      writer.u8(n & 255)
    }
    Short(n) => {
      require_range(n, -32768, 32767)
      writer.number((n & 65535).to_uint64(), 2)
    }
    UInt(n) => writer.number(n.to_uint64(), 4)
    ULong(n) => writer.number(n, 8)
    IntValue(n) => writer.number(n.reinterpret_as_uint().to_uint64(), 4)
    Long(n) | Timestamp(n) => writer.number(n.reinterpret_as_uint64(), 8)
    FloatValue(n) => writer.number(n.reinterpret_as_uint().to_uint64(), 4)
    DoubleValue(n) => writer.number(n.reinterpret_as_uint64(), 8)
    Decimal32(b) => fixed_bytes(writer, b, 4)
    Decimal64(b) => fixed_bytes(writer, b, 8)
    Decimal128(b) | Uuid(b) => fixed_bytes(writer, b, 16)
    Character(n) => {
      if n > 0x10ffffU || (n >= 0xd800U && n <= 0xdfffU) {
        raise Invalid("Unicode scalar", 0)
      }
      writer.number(n.to_uint64(), 4)
    }
    Binary(b) => {
      writer.number(b.length().to_uint64(), 4)
      writer.put(b)
    }
    Text(s) | Symbol(s) => {
      let b = text_bytes(s, maximum=writer.maximum - writer.bytes.length() - 4)
      if code == 0xb3 {
        for byte in b {
          if byte.to_int() > 127 {
            raise Invalid("non-ASCII symbol", 0)
          }
        }
      }
      writer.number(b.length().to_uint64(), 4)
      writer.put(b)
    }
    List(items) => write_sequence(writer, items, budget, depth)
    MapValue(entries) => {
      if entries.length() > budget.limits.max_items / 2 {
        raise Limit("map entries")
      }
      let body = wire_writer(budget.limits.max_value_bytes)
      body.number((entries.length() * 2).to_uint64(), 4)
      let seen : Map[Bytes, Bool] = Map([])
      for (key, item) in entries {
        let key_writer = wire_writer(budget.limits.max_value_bytes)
        write_value(key_writer, key, budget, depth + 1)
        let encoded_key = key_writer.finish()
        if seen.contains(encoded_key) {
          raise Invalid("duplicate map key value", 0)
        }
        seen[encoded_key] = true
        body.put(encoded_key)
        write_value(body, item, budget, depth + 1)
      }
      writer.number(body.bytes.length().to_uint64(), 4)
      writer.put(body.finish())
    }
    ArrayValue(element_code, descriptor, items) => {
      if items.length() > budget.limits.max_items {
        raise Limit("array entries")
      }
      let body = wire_writer(budget.limits.max_value_bytes)
      body.number(items.length().to_uint64(), 4)
      if element_code == 0 || element_code == 0x40 {
        raise Invalid("invalid array constructor", 0)
      }
      // Empty arrays still carry and validate the element constructor.
      if !canonical_array_code(element_code) {
        raise Unsupported("array constructor; use full-width code")
      }
      match descriptor {
        Some(d) => {
          if !descriptor_valid(d) {
            raise Invalid("array descriptor", 0)
          }
          body.u8(0)
          write_value(body, d, budget, depth + 1)
        }
        None => ()
      }
      body.u8(element_code)
      for item in items {
        budget.visit(depth + 1)
        write_body(body, element_code, item, budget, depth + 1)
      }
      writer.number(body.bytes.length().to_uint64(), 4)
      writer.put(body.finish())
    }
    Described(_, _) => raise Invalid("nested described array body", 0)
  }
}

///|
fn canonical_array_code(code : Int32) -> Bool {
  match code {
    0x50
    | 0x60
    | 0x70
    | 0x80
    | 0x51
    | 0x61
    | 0x71
    | 0x81
    | 0x56
    | 0x72
    | 0x82
    | 0x74
    | 0x84
    | 0x94
    | 0x73
    | 0x83
    | 0x98
    | 0xb0
    | 0xb1
    | 0xb3
    | 0xd0
    | 0xd1
    | 0xf0 => true
    _ => false
  }
}

///|
fn write_sequence(
  writer : WireWriter,
  items : Array[Value],
  budget : EncodeBudget,
  depth : Int32,
) -> Unit raise AmqpError {
  if items.length() > budget.limits.max_items {
    raise Limit("container entries")
  }
  let body = wire_writer(budget.limits.max_value_bytes)
  body.number(items.length().to_uint64(), 4)
  for item in items {
    write_value(body, item, budget, depth + 1)
  }
  writer.number(body.bytes.length().to_uint64(), 4)
  writer.put(body.finish())
}

///|
test "scalar encoder retains exact 64-bit unsigned and signed domains" {
  assert_eq(
    encode_value(ULong(18446744073709551615UL)),
    b"\x80\xff\xff\xff\xff\xff\xff\xff\xff",
  )
  assert_eq(encode_value(Long(-1L)), b"\x81\xff\xff\xff\xff\xff\xff\xff\xff")
  assert_eq(encode_value(Short(-2)), b"\x61\xff\xfe")
  assert_true(
    try {
      ignore(encode_value(UByte(256)))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(encode_value(Symbol("非ASCII")))
      false
    } catch {
      _ => true
    },
  )
}
