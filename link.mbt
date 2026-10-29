///|
priv struct PartialDelivery {
  id : UInt32
  tag : Bytes
  settled : Bool
  bytes : Array[Byte]
  mut fragments : Int32
}

///|
priv struct LinkState {
  name : String
  local_handle : UInt32
  mut remote_handle : UInt32?
  sender : Bool
  address : String
  mut ready : Bool
  mut closed : Bool
  mut delivery_count : UInt32
  mut credit : UInt32
  mut peer_max_message : UInt64
  mut partial : PartialDelivery?
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
    remote_handle: None,
    sender,
    address,
    ready: false,
    closed: false,
    delivery_count: 0U,
    credit: 0U,
    peer_max_message: 0UL,
    partial: None,
  }
}

///|
fn LinkState::attach(
  self : LinkState,
  perf : Performative,
) -> Unit raise AmqpError {
  if self.ready || self.closed {
    raise State("link already attached or closed")
  }
  if bool_field(perf, 2, false) != self.sender {
    raise Invalid("remote link role is not complementary", 0)
  }
  match perf.field(4) {
    Null | UByte(0) => ()
    _ => raise Unsupported("receiver settle mode second")
  }
  match perf.field(7) {
    Null => ()
    MapValue(entries) if entries.is_empty() => ()
    _ => raise Unsupported("unsettled link recovery")
  }
  if bool_field(perf, 8, false) {
    raise Unsupported("incomplete unsettled map")
  }
  let _ = terminus_address(perf.field(5), true)
  let _ = terminus_address(perf.field(6), false)
  let peer_max = match perf.field(10) {
    ULong(n) => n
    _ => 0UL
  }
  let count = if self.sender {
    self.delivery_count
  } else {
    uint_field(perf, 9, 0U)
  }
  self.remote_handle = Some(uint_field(perf, 1, 0U))
  self.peer_max_message = peer_max
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
    UByte(2),
    UByte(0),
    source(self.address),
    target(self.address),
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
