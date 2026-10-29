///|
fn validate_error(value : Value) -> Unit raise AmqpError {
  if value is Null {
    return
  }
  let fields = described_list(value, 0x1dUL, "amqp:error:list")
  if fields.length() < 1 || fields.length() > 3 || !(fields[0] is Symbol(_)) {
    raise Invalid("error requires symbolic condition", 0)
  }
  if !wire_kind(field_at(fields, 1), 1) || !wire_kind(field_at(fields, 2), 10) {
    raise Invalid("error description or info fields", 0)
  }
}

///|
test "error composites reject unrelated descriptors and preserve absent rejection error" {
  assert_true(
    try {
      ignore(Performative::new(0x18, [Described(ULong(0x24UL), List([]))]))
      false
    } catch {
      _ => true
    },
  )
  let value = Described(ULong(0x25UL), List([]))
  assert_eq(Outcome::from_value(value), RejectedWithoutError)
  assert_eq(
    encode_value(Outcome::from_value(value).value()),
    encode_value(value),
  )
  assert_true(
    try {
      ignore(
        Outcome::from_value(
          Described(ULong(0x27UL), List([Null, Null, MapValue([])])),
        ),
      )
      false
    } catch {
      _ => true
    },
  )
}
