pub suberror AmqpError {
  Invalid(String, Int)
  Unsupported(String)
  Limit(String)
  State(String)
} derive(Debug)

pub(all) struct Limits {
  max_value_bytes : Int
  max_frame_bytes : Int
  max_message_bytes : Int
  max_items : Int
  max_depth : Int
  max_links : Int
  max_pending : Int
  max_fragments : Int
} derive(Debug, Eq)

pub fn Limits::default() -> Limits {
  {
    max_value_bytes: 16777216, max_frame_bytes: 1048576,
    max_message_bytes: 16777216, max_items: 65536, max_depth: 32,
    max_links: 256, max_pending: 256, max_fragments: 4096,
  }
}

fn Limits::validate(self : Limits) -> Unit raise AmqpError {
  if self.max_value_bytes < 1 || self.max_frame_bytes < 512 ||
     self.max_message_bytes < 1 || self.max_items < 1 ||
     self.max_depth < 1 || self.max_depth > 128 ||
     self.max_links < 1 || self.max_pending < 1 || self.max_fragments < 1 {
    raise Limit("invalid resource limits")
  }
}

fn checked_add(a : Int, b : Int) -> Int raise AmqpError {
  if a < 0 || b < 0 || a > 2147483647 - b { raise Limit("integer sum") }
  a + b
}

fn minimum(a : Int, b : Int) -> Int { if a < b { a } else { b } }
