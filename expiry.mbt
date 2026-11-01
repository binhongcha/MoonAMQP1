///|
pub(all) struct ExpiryStatus {
  expired : Bool
  remaining_milliseconds : UInt64?
} derive(Debug)

///|
pub extend ExpiryStatus with @debug.Debug::{to_repr}

///|
/// Caller-supplied UTC milliseconds. TTL is measured since local acquisition;
/// absolute-expiry-time and TTL are both enforced, without adding timestamps.
pub fn Message::expiry_status(
  self : Message,
  acquired_at : Int64,
  now : Int64,
) -> ExpiryStatus raise AmqpError {
  if now < acquired_at {
    raise State("expiry clock moved backwards")
  }
  let elapsed = now.reinterpret_as_uint64() -
    acquired_at.reinterpret_as_uint64()
  let mut remaining : UInt64? = None
  for section in self.sections {
    if section.descriptor == 0x70 &&
      section.body is List(fields) &&
      fields.length() > 2 {
      if fields[2] is UInt(ttl) {
        remaining = Some(
          if elapsed >= ttl.to_uint64() {
            0UL
          } else {
            ttl.to_uint64() - elapsed
          },
        )
      }
    }
    if section.descriptor == 0x73 &&
      section.body is List(fields) &&
      fields.length() > 8 {
      if fields[8] is Timestamp(deadline) {
        let left = if now >= deadline {
          0UL
        } else {
          deadline.reinterpret_as_uint64() - now.reinterpret_as_uint64()
        }
        remaining = Some(
          match remaining {
            Some(r) => if r < left { r } else { left }
            None => left
          },
        )
      }
    }
  }
  { expired: remaining == Some(0UL), remaining_milliseconds: remaining, }
}
