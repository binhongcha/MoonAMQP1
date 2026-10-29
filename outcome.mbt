///|
pub enum Outcome {
  Accepted
  Rejected(String, String?)
  Released
  Modified(Bool, Bool)
} derive(Debug, Eq)

///|
pub extend Outcome with @debug.Debug::{to_repr}

///|
pub extend Outcome with Eq::{equal, not_equal}

///|
pub fn Outcome::value(self : Outcome) -> Value {
  match self {
    Accepted => Described(ULong(0x24UL), List([]))
    Rejected(condition, description) =>
      Described(
        ULong(0x25UL),
        List([
          Described(
            ULong(0x1dUL),
            List([
              Symbol(condition),
              description.map(s => Text(s)).unwrap_or(Null),
            ]),
          ),
        ]),
      )
    Released => Described(ULong(0x26UL), List([]))
    Modified(failed, undeliverable) =>
      Described(ULong(0x27UL), List([Boolean(failed), Boolean(undeliverable)]))
  }
}

///|
fn described_list(
  value : Value,
  expected : UInt64,
  name : String,
) -> Array[Value] raise AmqpError {
  match value {
    Described(ULong(code), List(items)) if code == expected => items
    Described(Symbol(symbol), List(items)) if symbol == name => items
    _ => raise Invalid("unexpected composite type", 0)
  }
}

///|
fn optional_boolean(value : Value) -> Bool raise AmqpError {
  match value {
    Null => false
    Boolean(b) => b
    _ => raise Invalid("expected boolean", 0)
  }
}

///|
pub fn Outcome::from_value(value : Value) -> Outcome raise AmqpError {
  let (code, items) = match value {
    Described(ULong(code), List(items)) => (code, items)
    Described(Symbol(symbol), List(items)) => {
      let code = match symbol {
        "amqp:accepted:list" => 0x24UL
        "amqp:rejected:list" => 0x25UL
        "amqp:released:list" => 0x26UL
        "amqp:modified:list" => 0x27UL
        _ => raise Unsupported("delivery state")
      }
      (code, items)
    }
    _ => raise Invalid("delivery outcome", 0)
  }
  match code {
    0x24UL | 0x26UL => {
      if !items.is_empty() {
        raise Invalid("empty outcome fields", 0)
      }
      if code == 0x24UL {
        Accepted
      } else {
        Released
      }
    }
    0x25UL => {
      if items.length() > 1 {
        raise Invalid("rejected fields", 0)
      }
      match field_at(items, 0) {
        Null => Rejected("amqp:internal-error", None)
        error => {
          let fields = described_list(error, 0x1dUL, "amqp:error:list")
          if fields.length() < 1 || fields.length() > 3 {
            raise Invalid("error fields", 0)
          }
          let condition = match fields[0] {
            Symbol(s) => s
            _ => raise Invalid("error condition", 0)
          }
          let description = match field_at(fields, 1) {
            Text(s) => Some(s)
            Null => None
            _ => raise Invalid("error description", 0)
          }
          if !wire_kind(field_at(fields, 2), 10) {
            raise Invalid("error info map", 0)
          }
          Rejected(condition, description)
        }
      }
    }
    0x27UL => {
      if items.length() > 3 {
        raise Invalid("modified fields", 0)
      }
      if !(field_at(items, 2) is Null) {
        validate_annotation(field_at(items, 2))
      }
      Modified(
        optional_boolean(field_at(items, 0)),
        optional_boolean(field_at(items, 1)),
      )
    }
    _ => raise Unsupported("non-terminal or transactional delivery state")
  }
}

///|
test "outcomes preserve semantic acceptance rejection release and modification" {
  let outcomes = [
    Accepted,
    Released,
    Rejected("amqp:not-found", Some("target")),
    Modified(true, false),
  ]
  for outcome in outcomes {
    assert_eq(
      Outcome::from_value(decode_value(encode_value(outcome.value()))),
      outcome,
    )
  }
}
