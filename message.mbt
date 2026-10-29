///|
pub(all) struct Section {
  descriptor : Int32
  body : Value
}

///|
pub struct Message {
  sections : Array[Section]
}

///|
fn section_value(section : Section) -> Value {
  Described(ULong(section.descriptor.to_uint64()), section.body)
}

///|
pub fn Message::new(
  sections : Array[Section],
  limits~ : Limits = Limits::default(),
) -> Message raise AmqpError {
  validate_sections(sections)
  let provisional = { sections, }
  // One budget spans all sections and the resulting immutable message is detached.
  decode_message(encode_message(provisional, limits~), limits~)
}

///|
pub fn Message::data(
  bytes : Bytes,
  properties~ : Array[(String, Value)] = [],
  limits~ : Limits = Limits::default(),
) -> Message raise AmqpError {
  let sections : Array[Section] = []
  if !properties.is_empty() {
    sections.push({
      descriptor: 0x74,
      body: MapValue(properties.map(pair => (Text(pair.0), pair.1))),
    })
  }
  sections.push({ descriptor: 0x75, body: Binary(bytes), })
  Message::new(sections, limits~)
}

///|
pub fn Message::sections(self : Message) -> Array[Section] {
  self.sections.map(s => {
    descriptor: s.descriptor,
    body: clone_value(s.body),
  })
}

///|
pub fn Message::data_bytes(self : Message) -> Bytes raise AmqpError {
  let output = wire_writer(2147483647)
  for section in self.sections {
    if section.descriptor == 0x75 {
      match section.body {
        Binary(bytes) => output.put(bytes)
        _ => abort("validated data section")
      }
    } else if section.descriptor == 0x76 || section.descriptor == 0x77 {
      raise State("message body is not data sections")
    }
  }
  output.finish()
}

///|
pub fn encode_message(
  message : Message,
  limits~ : Limits = Limits::default(),
) -> Bytes raise AmqpError {
  limits.validate()
  validate_sections(message.sections)
  let output = wire_writer(
    minimum(limits.max_message_bytes, limits.max_value_bytes),
  )
  let budget = { limits, nodes: 0, }
  for section in message.sections {
    write_value(output, section_value(section), budget, 0)
  }
  output.finish()
}

///|
pub fn decode_message(
  bytes : Bytes,
  limits~ : Limits = Limits::default(),
) -> Message raise AmqpError {
  if bytes.length() > limits.max_message_bytes {
    raise Limit("message bytes")
  }
  let reader = wire_reader(bytes, limits)
  let sections : Array[Section] = []
  while reader.pos < reader.end {
    let value = read_value(reader, 0)
    match value {
      Described(ULong(code), body) if code >= 0x70UL && code <= 0x78UL =>
        sections.push({ descriptor: code.to_int(), body, })
      Described(Symbol(name), body) => {
        let code = match name {
          "amqp:header:list" => 0x70
          "amqp:delivery-annotations:map" => 0x71
          "amqp:message-annotations:map" => 0x72
          "amqp:properties:list" => 0x73
          "amqp:application-properties:map" => 0x74
          "amqp:data:binary" => 0x75
          "amqp:amqp-sequence:list" => 0x76
          "amqp:amqp-value:*" => 0x77
          "amqp:footer:map" => 0x78
          _ => raise Unsupported("message section descriptor")
        }
        sections.push({ descriptor: code, body, })
      }
      _ =>
        raise Invalid("message section must have known descriptor", reader.pos)
    }
  }
  validate_sections(sections)
  { sections, }
}

///|
fn simple(value : Value) -> Bool {
  match value {
    List(_) | MapValue(_) | ArrayValue(_, _, _) | Described(_, _) => false
    _ => true
  }
}

///|
fn message_id(value : Value) -> Bool {
  match value {
    Null | ULong(_) | Uuid(_) | Binary(_) | Text(_) => true
    _ => false
  }
}

///|
fn validate_properties(value : Value) -> Unit raise AmqpError {
  let items = match value {
    List(items) => items
    _ => raise Invalid("properties list", 0)
  }
  let kinds = [0, 7, 1, 1, 1, 0, 12, 12, 13, 13, 1, 2, 1]
  if items.length() > kinds.length() {
    raise Unsupported("extra properties fields")
  }
  for i = 0; i < items.length(); i = i + 1 {
    let field = items[i]
    let valid = if kinds[i] == 0 {
      message_id(field)
    } else if kinds[i] == 12 {
      field is Null || field is Symbol(_)
    } else if kinds[i] == 13 {
      field is Null || field is Timestamp(_)
    } else {
      wire_kind(field, kinds[i])
    }
    if !valid {
      raise Invalid("message property field type {i}", 0)
    }
  }
}

///|
fn validate_annotation(value : Value) -> Unit raise AmqpError {
  match value {
    MapValue(entries) =>
      for (key, _) in entries {
        if !(key is Symbol(_) || key is ULong(_)) {
          raise Invalid("annotation key type", 0)
        }
      }
    _ => raise Invalid("annotation map", 0)
  }
}

///|
fn validate_sections(sections : Array[Section]) -> Unit raise AmqpError {
  let mut previous = 0x6f
  let mut body_kind = 0
  for section in sections {
    let code = section.descriptor
    if code < 0x70 ||
      code > 0x78 ||
      code < previous ||
      (code == previous && code != 0x75 && code != 0x76) {
      raise Invalid("message section order or duplicate", 0)
    }
    if code >= 0x75 && code <= 0x77 {
      if body_kind != 0 && body_kind != code {
        raise Invalid("mixed body section types", 0)
      }
      body_kind = code
    }
    match code {
      0x70 => {
        let items = match section.body {
          List(items) => items
          _ => raise Invalid("header list", 0)
        }
        let kinds = [4, 5, 2, 4, 2]
        if items.length() > 5 {
          raise Unsupported("extra header fields")
        }
        for i = 0; i < items.length(); i = i + 1 {
          if !wire_kind(items[i], kinds[i]) {
            raise Invalid("header field type", 0)
          }
        }
      }
      0x71 | 0x72 | 0x78 => validate_annotation(section.body)
      0x73 => validate_properties(section.body)
      0x74 =>
        match section.body {
          MapValue(entries) =>
            for (key, value) in entries {
              if !(key is Text(_)) || !simple(value) {
                raise Invalid(
                  "application properties require string keys and simple values",
                  0,
                )
              }
            }
          _ => raise Invalid("application properties map", 0)
        }
      0x75 =>
        if !(section.body is Binary(_)) {
          raise Invalid("data body must be binary", 0)
        }
      0x76 =>
        if !(section.body is List(_)) {
          raise Invalid("sequence body must be list", 0)
        }
      _ => ()
    }
    previous = code
  }
}

///|
test "message section order body type and typed properties are strict" {
  let message = Message::data(b"payload", properties=[
    ("id", ULong(18446744073709551615UL)),
  ])
  assert_eq(decode_message(encode_message(message)).data_bytes(), b"payload")
  assert_true(
    try {
      ignore(
        Message::new([
          { descriptor: 0x75, body: Binary(b"x"), },
          { descriptor: 0x73, body: List([]), },
        ]),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(
        Message::new([
          { descriptor: 0x75, body: Binary(b"x"), },
          { descriptor: 0x77, body: Null, },
        ]),
      )
      false
    } catch {
      _ => true
    },
  )
  assert_true(
    try {
      ignore(Message::new([{ descriptor: 0x73, body: List([UInt(1U)]), }]))
      false
    } catch {
      _ => true
    },
  )
}
