///|
/// Non-durable, non-dynamic termini. Applications own node creation and lifetime.
pub fn source(address : String) -> Value {
  Described(ULong(0x28UL), List([Text(address)]))
}

///|
pub fn target(address : String) -> Value {
  Described(ULong(0x29UL), List([Text(address)]))
}

///|
fn terminus_address(
  value : Value,
  receiver_source : Bool,
) -> String? raise AmqpError {
  if value is Null {
    return None
  }
  let items = if receiver_source {
    described_list(value, 0x28UL, "amqp:source:list")
  } else {
    described_list(value, 0x29UL, "amqp:target:list")
  }
  let maximum = if receiver_source { 11 } else { 7 }
  if items.length() > maximum {
    raise Invalid("terminus field count", 0)
  }
  let address = match field_at(items, 0) {
    Null => None
    Text(s) => Some(s)
    _ => raise Invalid("terminus address", 0)
  }
  // The endpoint profile cannot honor durable/dynamic/resumable/filter termini.
  match field_at(items, 1) {
    Null | UInt(0U) => ()
    _ => raise Unsupported("durable terminus")
  }
  match field_at(items, 2) {
    Null
    | Symbol("session-end")
    | Symbol("link-detach")
    | Symbol("connection-close")
    | Symbol("never") => ()
    _ => raise Invalid("terminus expiry policy", 0)
  }
  if !wire_kind(field_at(items, 3), 2) {
    raise Invalid("terminus timeout", 0)
  }
  if optional_boolean(field_at(items, 4)) {
    raise Unsupported("dynamic terminus")
  }
  if !(field_at(items, 5) is Null) {
    raise Unsupported("dynamic node properties")
  }
  if receiver_source {
    if !(field_at(items, 6) is Null) ||
      !(field_at(items, 7) is Null) ||
      !(field_at(items, 8) is Null) ||
      !(field_at(items, 9) is Null) ||
      !(field_at(items, 10) is Null) {
      raise Unsupported("source distribution filters outcomes or capabilities")
    }
  } else if !(field_at(items, 6) is Null) {
    raise Unsupported("target capabilities")
  }
  address
}

///|
test "source target addresses and unsupported dynamic nodes" {
  assert_eq(terminus_address(source("jobs"), true), Some("jobs"))
  assert_eq(terminus_address(target("telemetry"), false), Some("telemetry"))
  assert_true(
    try {
      ignore(
        terminus_address(
          Described(
            ULong(0x29UL),
            List([Text("x"), Null, Null, Null, Boolean(true)]),
          ),
          false,
        ),
      )
      false
    } catch {
      _ => true
    },
  )
}
