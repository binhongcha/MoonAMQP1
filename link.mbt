///|
#derive(Eq)
pub(all) enum ReceiverSettleMode {
  First
  Second
}

///|
pub extend ReceiverSettleMode with Eq::{equal, not_equal}

///|
priv struct PartialDelivery {
  id : UInt32
  tag : Bytes
  settled : Bool
  receiver_mode : ReceiverSettleMode
  bytes : Array[Byte]
  mut fragments : Int32
  mut skip : Int32
  ignored : Bool
}

///|
priv struct LinkState {
  name : String
  mut local_handle : UInt32
  mut pause_pending : Bool
  mut suspended : Bool
  mut resume_pending : Bool
  mut remote_handle : UInt32?
  sender : Bool
  address : String
  mut ready : Bool
  mut closed : Bool
  mut failed : Bool
  mut failed_deliveries : Array[UInt32]
  mut delivery_count : UInt32
  mut credit : UInt32
  mut peer_max_message : UInt64
  mut drain_mode : Bool
  mut draining : Bool
  mut drain_returned : UInt32
  mut receiver_mode : ReceiverSettleMode
  mut settle_mode : Int32
  mut partial : PartialDelivery?
  mut recoverable : Bool
  records : Map[Bytes, RecoveryDelivery]
  mut peer_records : Map[Bytes, Value]
  mut recovery_cursor : Int32
  mut sent_incomplete : Bool
  mut peer_incomplete : Bool
}

///|
fn link_state(
  name : String,
  local_handle : UInt32,
  sender : Bool,
  address : String,
) -> LinkState {
  {
    name,
    local_handle,
    pause_pending: false,
    suspended: false,
    resume_pending: false,
    remote_handle: None,
    sender,
    address,
    ready: false,
    closed: false,
    failed: false,
    failed_deliveries: [],
    delivery_count: 0U,
    credit: 0U,
    peer_max_message: 0UL,
    drain_mode: false,
    draining: false,
    drain_returned: 0U,
    receiver_mode: First,
    settle_mode: 2,
    partial: None,
    recoverable: false,
    records: Map([]),
    peer_records: Map([]),
    recovery_cursor: 0,
    sent_incomplete: false,
    peer_incomplete: false,
  }
}

///|
fn LinkState::attach(
  self : LinkState,
  perf : Performative,
  responding~ : Bool = false,
) -> Unit raise AmqpError {
  if self.ready || self.closed {
    raise State("link already attached or closed")
  }
  if bool_field(perf, 2, false) != self.sender {
    raise Invalid("remote link role is not complementary", 0)
  }
  let peer_records = decode_unsettled(perf.field(7))
  self.peer_records = peer_records
  self.peer_incomplete = bool_field(perf, 8, false)
  let _ = terminus_address(perf.field(5), true)
  let _ = terminus_address(perf.field(6), false)
  let peer_max = match perf.field(10) {
    ULong(n) => n
    _ => 0UL
  }
  let mode = match perf.field(3) {
    UByte(n) => n
    _ => 2
  }
  let count = if self.sender {
    self.delivery_count
  } else {
    uint_field(perf, 9, 0U)
  }
  if !self.recoverable &&
    (self.suspended || self.resume_pending) &&
    count != self.delivery_count {
    raise Invalid("resumed quiescent link counter differs", 0)
  }
  // The receiver announces the actual receiver settlement mode.
  if self.sender || !responding {
    self.receiver_mode = if perf.field(4) is UByte(1) { Second } else { First }
  }
  self.resume_pending = false
  self.remote_handle = Some(uint_field(perf, 1, 0U))
  self.peer_max_message = peer_max
  self.settle_mode = mode
  self.delivery_count = count
  self.ready = true
}

///|
fn LinkState::update_credit(
  self : LinkState,
  delivery_count : UInt32,
  credit : UInt32,
) -> Unit raise AmqpError {
  bounded_window(credit)
  let spent = serial_distance(delivery_count, self.delivery_count)
  self.credit = if spent > credit { 0U } else { credit - spent }
}

///|
fn LinkState::attach_fields(self : LinkState, limits : Limits) -> Array[Value] {
  [
    Text(self.name),
    UInt(self.local_handle),
    Boolean(!self.sender),
    UByte(self.settle_mode),
    UByte(if self.receiver_mode == Second { 1 } else { 0 }),
    if self.recoverable {
      persistent_terminus(self.address, true)
    } else {
      source(self.address)
    },
    if self.recoverable {
      persistent_terminus(self.address, false)
    } else {
      target(self.address)
    },
    Null,
    Boolean(false),
    if self.sender {
      UInt(self.delivery_count)
    } else {
      Null
    },
    ULong(limits.max_message_bytes.to_uint64()),
  ]
}

///|
test "link credit is delivery-based and uses remote delivery-count" {
  let link = link_state("work", 1U, true, "jobs")
  link.delivery_count = 10U
  link.update_credit(8U, 5U)
  assert_eq(link.credit, 3U)
  link.delivery_count = 13U
  link.update_credit(10U, 2U)
  assert_eq(link.credit, 0U)
}
