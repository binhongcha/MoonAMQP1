///|
#derive(Debug, Eq)
pub(all) struct SuspendedLink {
  name : String
  address : String
  sender : Bool
  delivery_count : UInt32
}

///|
pub extend SuspendedLink with Eq::{equal, not_equal}

///|
pub extend SuspendedLink with @debug.Debug::{to_repr}

///|
pub fn Endpoint::suspended_links(
  self : Endpoint,
  channel~ : Int32 = self.channel,
) -> Array[SuspendedLink] raise AmqpError {
  let snapshot : Array[SuspendedLink] = []
  for link in self.session_active(channel).links {
    if link.suspended {
      snapshot.push({
        name: link.name,
        address: link.address,
        sender: link.sender,
        delivery_count: link.delivery_count,
      })
    }
  }
  snapshot
}
