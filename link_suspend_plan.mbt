///|
#derive(Eq, Debug)
pub(all) enum SuspendReadiness {
  SuspendReady
  LinkNotReady
  ReceiverHasCredit
  DeliveryInProgress
}

///|
pub extend SuspendReadiness with Eq::{equal, not_equal}

///|
pub extend SuspendReadiness with @debug.Debug::{to_repr}

///|
/// Determine if the link is quiescent before scheduling a detach.
pub fn Endpoint::suspend_readiness(
  self : Endpoint,
  handle : UInt32,
  channel~ : Int32 = self.channel,
) -> SuspendReadiness raise AmqpError {
  let session = self.session_active(channel)
  let link = session.local_link(handle)
  if !link.ready {
    return LinkNotReady
  }
  if !link.recoverable && !link.sender && link.credit != 0U {
    return ReceiverHasCredit
  }
  if link.recoverable {
    return SuspendReady
  }
  try {
    session.check_quiescent(link)
    SuspendReady
  } catch {
    Unsupported(_) => DeliveryInProgress
    e => raise e
  }
}
