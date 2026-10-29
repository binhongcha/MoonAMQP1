///|
pub suberror AmqpError {
  Invalid(String, Int32)
  Unsupported(String)
  Limit(String)
  State(String)
} derive(Debug)

///|
pub extend AmqpError with @debug.Debug::{to_repr}

///|
pub(all) struct Limits {
  max_value_bytes : Int32
  max_frame_bytes : Int32
  max_message_bytes : Int32
  max_items : Int32
  max_depth : Int32
  max_links : Int32
  max_pending : Int32
  max_fragments : Int32
} derive(Debug, Eq)

///|
pub extend Limits with @debug.Debug::{to_repr}

///|
pub extend Limits with Eq::{equal, not_equal}

///|
pub fn Limits::default() -> Limits {
  {
    max_value_bytes: 16777216,
    max_frame_bytes: 1048576,
    max_message_bytes: 16777216,
    max_items: 65536,
    max_depth: 32,
    max_links: 256,
    max_pending: 256,
    max_fragments: 4096,
  }
}

///|
fn Limits::validate(self : Limits) -> Unit raise AmqpError {
  if self.max_value_bytes < 1 ||
    self.max_frame_bytes < 512 ||
    self.max_message_bytes < 1 ||
    self.max_items < 1 ||
    self.max_depth < 1 ||
    self.max_depth > 128 ||
    self.max_links < 1 ||
    self.max_pending < 1 ||
    self.max_fragments < 1 {
    raise Limit("invalid resource limits")
  }
}

///|
fn checked_add(a : Int32, b : Int32) -> Int32 raise AmqpError {
  if a < 0 || b < 0 || a > 2147483647 - b {
    raise Limit("integer sum")
  }
  a + b
}

///|
fn minimum(a : Int32, b : Int32) -> Int32 {
  if a < b {
    a
  } else {
    b
  }
}
