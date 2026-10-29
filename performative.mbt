///|
/// Validated transport list. Accessors return detached snapshots.
pub struct Performative {
  priv code : Int32
  priv fields : Array[Value]
}

///|
fn clone_value(value : Value) -> Value {
  match value {
    List(items) => List(items.map(clone_value))
    MapValue(items) =>
      MapValue(items.map(pair => (clone_value(pair.0), clone_value(pair.1))))
    ArrayValue(code, descriptor, items) =>
      ArrayValue(code, descriptor.map(clone_value), items.map(clone_value))
    Described(d, v) => Described(clone_value(d), clone_value(v))
    _ => value
  }
}

///|
pub fn Performative::new(
  code : Int32,
  fields : Array[Value],
  limits~ : Limits = Limits::default(),
) -> Performative raise AmqpError {
  // Validate and copy through a bounded representation, including caller-owned arrays.
  let frozen = decode_value(encode_value(List(fields), limits~), limits~)
  let items = match frozen {
    List(items) => items
    _ => abort("list invariant")
  }
  validate_performative(code, items)
  { code, fields: items, }
}

///|
pub fn Performative::descriptor(self : Performative) -> Int32 {
  self.code
}

///|
pub fn Performative::field(self : Performative, index : Int32) -> Value {
  if index < 0 || index >= self.fields.length() {
    Null
  } else {
    clone_value(self.fields[index])
  }
}

///|
pub fn Performative::value(self : Performative) -> Value {
  Described(ULong(self.code.to_uint64()), List(self.fields.map(clone_value)))
}

///|
pub fn Performative::from_value(
  value : Value,
  limits~ : Limits = Limits::default(),
) -> Performative raise AmqpError {
  match value {
    Described(descriptor, List(fields)) => {
      let code = match descriptor {
        ULong(n) if n >= 0x10UL && n <= 0x18UL => n.to_int()
        Symbol(name) =>
          match name {
            "amqp:open:list" => 0x10
            "amqp:begin:list" => 0x11
            "amqp:attach:list" => 0x12
            "amqp:flow:list" => 0x13
            "amqp:transfer:list" => 0x14
            "amqp:disposition:list" => 0x15
            "amqp:detach:list" => 0x16
            "amqp:end:list" => 0x17
            "amqp:close:list" => 0x18
            _ => raise Unsupported("unknown performative descriptor")
          }
        _ => raise Unsupported("performative descriptor")
      }
      Performative::new(code, fields, limits~)
    }
    _ => raise Invalid("performative must be described list", 0)
  }
}

///|
fn field_at(fields : Array[Value], i : Int32) -> Value {
  if i < fields.length() {
    fields[i]
  } else {
    Null
  }
}

///|
fn wire_kind(value : Value, kind : Int32) -> Bool {
  if value is Null {
    return true
  }
  match (kind, value) {
    (1, Text(_))
    | (2, UInt(_))
    | (3, UShort(_))
    | (4, Boolean(_))
    | (5, UByte(_))
    | (6, ULong(_))
    | (7, Binary(_))
    | (8, Described(_, _)) => true
    (9, ArrayValue(0xb3, None, _)) => true
    (10, MapValue(entries)) => entries.all(pair => pair.0 is Symbol(_))
    (11, MapValue(_)) => true
    _ => false
  }
}

///|
fn validate_performative(
  code : Int32,
  fields : Array[Value],
) -> Unit raise AmqpError {
  let (kinds, mandatory) : (Array[Int32], Array[Int32]) = match code {
    0x10 => ([1, 1, 2, 3, 2, 9, 9, 9, 9, 10], [0])
    0x11 => ([3, 2, 2, 2, 2, 9, 9, 10], [1, 2, 3])
    0x12 => ([1, 2, 4, 5, 5, 8, 8, 11, 4, 2, 6, 9, 9, 10], [0, 1, 2])
    0x13 => ([2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 10], [1, 2, 3])
    0x14 => ([2, 2, 7, 2, 4, 4, 5, 8, 4, 4, 4], [0])
    0x15 => ([4, 2, 2, 4, 8, 4], [0, 1])
    0x16 => ([2, 4, 8], [0])
    0x17 | 0x18 => ([8], [])
    _ => raise Unsupported("transport descriptor")
  }
  if fields.length() > kinds.length() {
    raise Unsupported("extra performative fields")
  }
  for index in mandatory {
    if field_at(fields, index) is Null {
      raise Invalid("missing mandatory performative field {index}", 0)
    }
  }
  for i = 0; i < fields.length(); i = i + 1 {
    if !wire_kind(fields[i], kinds[i]) {
      raise Invalid("performative field type {i}", 0)
    }
  }
  if code == 0x10 {
    if field_at(fields, 2) is UInt(n) && n < 512U {
      raise Invalid("max-frame-size below 512", 0)
    }
  }
  if code == 0x12 {
    if field_at(fields, 3) is UByte(n) && n > 2 {
      raise Invalid("sender settle mode", 0)
    }
    if field_at(fields, 4) is UByte(n) && n > 1 {
      raise Invalid("receiver settle mode", 0)
    }
    if field_at(fields, 2) is Boolean(false) && field_at(fields, 9) is Null {
      raise Invalid("sender initial-delivery-count required", 0)
    }
  }
  if code == 0x14 {
    if field_at(fields, 6) is UByte(n) && n > 1 {
      raise Invalid("transfer settle mode", 0)
    }
    if field_at(fields, 2) is Binary(tag) && tag.length() > 32 {
      raise Invalid("delivery tag exceeds 32 bytes", 0)
    }
  }
}

///|
fn uint_field(perf : Performative, index : Int32, fallback : UInt32) -> UInt32 {
  match field_at(perf.fields, index) {
    UInt(n) => n
    _ => fallback
  }
}

///|
fn bool_field(perf : Performative, index : Int32, fallback : Bool) -> Bool {
  match field_at(perf.fields, index) {
    Boolean(n) => n
    _ => fallback
  }
}

///|
test "mandatory transport fields and sender delivery count are enforced" {
  assert_true(
    try {
      ignore(Performative::new(0x10, []))
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(Performative::new(0x12, [Text("link"), UInt(0U), Boolean(false)]))
      false
    } catch {
      _ => true
    },
  )
  let open = Performative::new(0x10, [Text("local"), Null, UInt(512U)])
  assert_eq(Performative::from_value(open.value()).descriptor(), 0x10)
  let fields = [Text("frozen")]
  let frozen = Performative::new(0x10, fields)
  fields[0] = Text("changed")
  assert_eq(frozen.field(0).as_text(), "frozen")
}
