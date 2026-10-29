///|
test "all primitive domains round-trip canonical bytes including nonfinite bits" {
  let values : Array[Value] = [
    Null,
    Boolean(false),
    Boolean(true),
    UByte(255),
    UShort(65535),
    UInt(4294967295U),
    ULong(18446744073709551615UL),
    ByteValue(-128),
    Short(-32768),
    IntValue(-2147483648),
    Long(-9223372036854775808L),
    Timestamp(-123456789L),
    FloatValue(Float::reinterpret_from_uint(0x7fc01234U)),
    FloatValue(-0.0F),
    DoubleValue(0x7ff8000000001234UL.reinterpret_as_double()),
    DoubleValue(-0.0),
    Decimal32(b"\x01\x02\x03\x04"),
    Decimal64(b"12345678"),
    Decimal128(b"1234567890123456"),
    Character(0x1f680U),
    Uuid(b"abcdefghijklmnop"),
    Binary(b"\x00\xff"),
    Text("MoonBit 月 🌙"),
    Symbol("amqp:accepted:list"),
    List([]),
    MapValue([]),
  ]
  for value in values {
    let encoded = encode_value(value)
    assert_eq(encode_value(decode_value(encoded)), encoded)
  }
}

///|
test "homogeneous arrays retain constructor for empty and described arrays" {
  let arrays : Array[Value] = [
    ArrayValue(0x70, None, []),
    ArrayValue(0x70, None, [UInt(0U), UInt(4294967295U)]),
    ArrayValue(0xb3, None, [Symbol("one"), Symbol("two")]),
    ArrayValue(0xd0, Some(ULong(0x24UL)), [List([]), List([])]),
    ArrayValue(0xf0, None, [ArrayValue(0x80, None, [ULong(1UL)])]),
  ]
  for array in arrays {
    let bytes = encode_value(array)
    assert_eq(encode_value(decode_value(bytes)), bytes)
  }
  // array8 smalluint constructor, no per-element constructor bytes.
  let compact = decode_value(b"\xe0\x04\x02\x52\x01\xff")
  assert_eq(
    encode_value(compact),
    encode_value(ArrayValue(0x70, None, [UInt(1U), UInt(255U)])),
  )
}

///|
test "every truncated prefix of a nested value fails without a partial result" {
  let data = encode_value(
    Described(
      Symbol("example:envelope"),
      List([
        MapValue([(Text("sensor"), ULong(99UL))]),
        ArrayValue(0xb0, None, [Binary(b"abc")]),
      ]),
    ),
  )
  for size = 0; size < data.length(); size = size + 1 {
    assert_true(
      try {
        ignore(decode_value(data[:size].to_owned()))
        false
      } catch {
        _ => true
      },
    )
  }
  assert_eq(encode_value(decode_value(data)), data)
}

///|
test "container limits prevent tiny zero-width array amplification" {
  let limits = { ..Limits::default(), max_items: 4, }
  assert_true(
    try {
      ignore(decode_value(b"\xe0\x02\xff\x41", limits~))
      false
    } catch {
      _ => true
    },
  )
  let deep = Described(ULong(1UL), Described(ULong(2UL), List([Null])))
  assert_true(
    try {
      ignore(encode_value(deep, limits={ ..limits, max_depth: 1, }))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(decode_value(b"\x73\x00\x00\xd8\x00"))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(decode_value(b"\xa1\x02\xc0\x80"))
      false
    } catch {
      _ => true
    },
  )
}

///|
test "map keys retain order allow null and reject duplicate values across compact forms" {
  let value = MapValue([
    (Null, Text("null-key")),
    (UInt(1U), Text("uint")),
    (ULong(1UL), Text("ulong")),
  ])
  let bytes = encode_value(value)
  assert_eq(encode_value(decode_value(bytes)), bytes)
  assert_true(
    try {
      ignore(encode_value(MapValue([(UInt(1U), Null), (UInt(1U), Null)])))
      false
    } catch {
      _ => true
    },
  )
  // uint0 and full-width uint(0) are one key, not two distinct byte strings.
  assert_true(
    try {
      ignore(decode_value(b"\xc1\x09\x04\x43\x40\x70\x00\x00\x00\x00\x40"))
      false
    } catch {
      _ => true
    },
  )
  let open = Performative::new(0x10, [
    Text("single-capability"),
    Null,
    Null,
    Null,
    Null,
    Null,
    Null,
    Symbol("capability"),
  ])
  assert_true(open.field(7) is Symbol(_))
  assert_eq(
    encode_value(decode_value(b"\xe0\x02\x03\x41")),
    encode_value(
      ArrayValue(0x56, None, [Boolean(true), Boolean(true), Boolean(true)]),
    ),
  )
}

///|
test "Unicode surrogates fail consistently and BOM plus ASCII NUL remain data" {
  let broken = "🌙".unsafe_substring(start=0, end=1)
  assert_true(
    try {
      ignore(encode_value(Text(broken)))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(decode_value(b"\xa1\x03\xed\xa0\x80"))
      false
    } catch {
      _ => true
    },
  )
  let bom = decode_value(b"\xa1\x04\xef\xbb\xbf\x61")
  assert_eq(bom.as_text(), "\uFEFFa")
  let nul_symbol = Symbol("a\u0000b")
  assert_eq(
    encode_value(decode_value(encode_value(nul_symbol))),
    encode_value(nul_symbol),
  )
  assert_true(
    try {
      ignore(
        encode_value(Text("🌙🌙"), limits={
          ..Limits::default(),
          max_value_bytes: 8,
        }),
      )
      false
    } catch {
      _ => true
    },
  )
}
