///|
fn Endpoint::flow_frame(
  self : Endpoint,
  link : LinkState?,
) -> Bytes raise AmqpError {
  let (incoming, outgoing, _) = self.window.remaining()
  let fields = [
    UInt(self.window.next_in),
    UInt(incoming),
    UInt(self.window.next_out),
    UInt(outgoing),
  ]
  match link {
    Some(link) => {
      fields.push(UInt(link.local_handle))
      fields.push(UInt(link.delivery_count))
      fields.push(UInt(link.credit))
    }
    None => ()
  }
  self.frame(0x13, fields)
}

///|
pub fn Endpoint::grant(
  self : Endpoint,
  handle : UInt32,
  credit : UInt32,
) -> Unit raise AmqpError {
  self.session_active()
  let link = self.local_link(handle)
  if link.sender || !link.ready {
    raise State("grant needs attached receiver link")
  }
  bounded_window(credit)
  if credit.to_uint64() > self.limits.max_pending.to_uint64() {
    raise Limit("credit above pending budget")
  }
  let previous = link.credit
  link.credit = credit
  errdefer {
    link.credit = previous
  }
  self.queue(self.flow_frame(Some(link)))
}

///|
pub fn Endpoint::refresh_windows(
  self : Endpoint,
  incoming : UInt32,
  outgoing : UInt32,
) -> Unit raise AmqpError {
  self.session_active()
  let (old_in, old_out, _) = self.window.remaining()
  self.window.refresh(incoming, outgoing)
  errdefer (self.window.refresh(old_in, old_out) catch {
    _ => abort("window invariant")
  })
  self.queue(self.flow_frame(None))
}

///|
fn Endpoint::receive_flow(
  self : Endpoint,
  perf : Performative,
) -> Unit raise AmqpError {
  if bool_field(perf, 8, false) {
    raise Unsupported("link drain flow")
  }
  let next_in = match perf.field(0) {
    UInt(n) => Some(n)
    _ => Some(0U)
  }
  self.window.update_peer(
    next_in,
    uint_field(perf, 1, 0U),
    uint_field(perf, 2, 0U),
    uint_field(perf, 3, 0U),
  )
  match perf.field(4) {
    Null => ()
    UInt(handle) => {
      let link = self.remote_link(handle)
      if !link.ready {
        raise State("flow before attach")
      }
      if link.sender {
        let count = match perf.field(5) {
          UInt(n) => n
          _ => raise Invalid("link flow delivery-count required", 0)
        }
        link.update_credit(count, uint_field(perf, 6, 0U))
        self.event(Credit(link.local_handle, link.credit))
      }
    }
    _ => raise Invalid("flow handle", 0)
  }
  if bool_field(perf, 9, false) {
    self.queue(self.flow_frame(None))
  }
}
