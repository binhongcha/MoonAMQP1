///|
/// Exact wire domains; ULong/Long never travel through Double.
pub enum Value {
  Null
  Boolean(Bool)
  UByte(Int32)
  UShort(Int32)
  UInt(UInt32)
  ULong(UInt64)
  ByteValue(Int32)
  Short(Int32)
  IntValue(Int32)
  Long(Int64)
  FloatValue(Float)
  DoubleValue(Double)
  Decimal32(Bytes)
  Decimal64(Bytes)
  Decimal128(Bytes)
  Character(UInt32)
  Timestamp(Int64)
  Uuid(Bytes)
  Binary(Bytes)
  Text(String)
  Symbol(String)
  List(Array[Value])
  MapValue(Array[(Value, Value)])
  ArrayValue(Int32, Value?, Array[Value])
  Described(Value, Value)
} derive(Debug)

///|
pub extend Value with @debug.Debug::{to_repr}

///|
fn descriptor_valid(value : Value) -> Bool {
  match value {
    ULong(_) | Symbol(_) => true
    _ => false
  }
}

///|
fn value_code(value : Value) -> Int32 {
  match value {
    Null => 0x40
    Boolean(_) => 0x56
    UByte(_) => 0x50
    UShort(_) => 0x60
    UInt(_) => 0x70
    ULong(_) => 0x80
    ByteValue(_) => 0x51
    Short(_) => 0x61
    IntValue(_) => 0x71
    Long(_) => 0x81
    FloatValue(_) => 0x72
    DoubleValue(_) => 0x82
    Decimal32(_) => 0x74
    Decimal64(_) => 0x84
    Decimal128(_) => 0x94
    Character(_) => 0x73
    Timestamp(_) => 0x83
    Uuid(_) => 0x98
    Binary(_) => 0xb0
    Text(_) => 0xb1
    Symbol(_) => 0xb3
    List(_) => 0xd0
    MapValue(_) => 0xd1
    ArrayValue(_, _, _) => 0xf0
    Described(_, _) => 0
  }
}

///|

///|
pub fn Value::as_text(self : Value) -> String raise AmqpError {
  match self {
    Text(text) | Symbol(text) => text
    _ => raise Invalid("expected text or symbol", 0)
  }
}

///|
pub fn Value::as_uint(self : Value) -> UInt32 raise AmqpError {
  match self {
    UInt(value) => value
    _ => raise Invalid("expected uint", 0)
  }
}

///|
test "wire type identity distinguishes unsigned long from numeric approximations" {
  assert_eq(value_code(ULong(18446744073709551615UL)), 0x80)
  assert_true(descriptor_valid(Symbol("example:type")))
  assert_false(descriptor_valid(UInt(7U)))
}
