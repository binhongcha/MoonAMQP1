///|
#derive(Debug, Eq)
pub(all) enum Outcome {
  Accepted
  Rejected(String, String?)
  RejectedWithoutError
  Released
  Modified(Bool, Bool)
  ModifiedWithAnnotations(Bool, Bool, OutcomeAnnotations)
}

///|
pub extend Outcome with @debug.Debug::{to_repr}

///|
pub extend Outcome with Eq::{equal, not_equal}

///|
pub fn Outcome::value(self : Outcome) -> Value {
  match self {
    Accepted => Described(ULong(0x24UL), List([]))
    RejectedWithoutError => Described(ULong(0x25UL), List([]))
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
    ModifiedWithAnnotations(failed, undeliverable, annotations) =>
      Described(
        ULong(0x27UL),
        List([Boolean(failed), Boolean(undeliverable), annotations.value()]),
      )
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
        Null => RejectedWithoutError
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
      let failed = optional_boolean(field_at(items, 0))
      let undeliverable = optional_boolean(field_at(items, 1))
      match field_at(items, 2) {
        Null => Modified(failed, undeliverable)
        annotations =>
          ModifiedWithAnnotations(
            failed,
            undeliverable,
            OutcomeAnnotations::from_value(annotations),
          )
      }
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

///|
/// Symbols and their typed values carried in the Modified outcome.
#derive(Debug)
pub struct OutcomeAnnotations {
  priv items : Value
  priv canonical : Bytes
}

///|
pub extend OutcomeAnnotations with @debug.Debug::{to_repr}

///|
pub extend OutcomeAnnotations with Eq::{equal, not_equal}

///|
pub impl Eq for OutcomeAnnotations with fn equal(self, other) {
  self.canonical == other.canonical
}

///|
pub fn OutcomeAnnotations::new(
  pairs : Array[(String, Value)],
) -> OutcomeAnnotations raise AmqpError {
  let sorted = pairs.copy()
  sorted.sort_by((a, b) => a.0.compare(b.0))
  let items = MapValue(sorted.map(pair => (Symbol(pair.0), pair.1)))
  let canonical = encode_value(items)
  { items: decode_value(canonical), canonical, }
}

///|
pub fn OutcomeAnnotations::value(self : OutcomeAnnotations) -> Value {
  clone_value(self.items)
}

///|
fn OutcomeAnnotations::from_value(
  value : Value,
) -> OutcomeAnnotations raise AmqpError {
  guard value is MapValue(entries) else {
    raise Invalid("Modified message-annotations requires fields", 0)
  }
  let pairs : Array[(String, Value)] = []
  for (key, item) in entries {
    guard key is Symbol(name) else {
      raise Invalid("Modified message-annotations keys must be symbols", 0)
    }
    pairs.push((name, item))
  }
  OutcomeAnnotations::new(pairs)
}

///|
test "modified outcomes preserve typed annotations and compare unordered keys" {
  let annotations = OutcomeAnnotations::new([
    ("z", UInt(13U)),
    ("a", Binary(b"route")),
  ])
  let equivalent = OutcomeAnnotations::new([
    ("a", Binary(b"route")),
    ("z", UInt(13U)),
  ])
  let state = ModifiedWithAnnotations(true, false, annotations)
  assert_eq(state, ModifiedWithAnnotations(true, false, equivalent))
  assert_eq(
    Outcome::from_value(decode_value(encode_value(state.value()))),
    state,
  )
  assert_true(
    try {
      ignore(
        OutcomeAnnotations::new([("duplicate", Null), ("duplicate", Null)]),
      )
      false
    } catch {
      Invalid(_, _) => true
      _ => false
    },
  )
}
