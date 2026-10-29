///|
/// Serial distance in the unambiguous half of the 32-bit sequence space.
pub fn serial_distance(first : UInt32, last : UInt32) -> UInt32 raise AmqpError {
  let distance = last - first
  if distance >= 0x80000000U {
    raise Invalid("ambiguous or backwards serial distance", 0)
  }
  distance
}

///|
fn bounded_window(value : UInt32) -> Unit raise AmqpError {
  if value >= 0x80000000U {
    raise Limit("window or credit must be below 2^31")
  }
}

///|
pub struct SessionWindow {
  priv mut next_out : UInt32
  priv mut next_in : UInt32
  priv mut incoming : UInt32
  priv mut outgoing : UInt32
  priv mut peer_incoming : UInt32
  priv mut peer_outgoing : UInt32
  priv mut established : Bool
}

///|
pub fn SessionWindow::new(
  incoming : UInt32,
  outgoing : UInt32,
  next_outgoing~ : UInt32 = 0U,
) -> SessionWindow raise AmqpError {
  bounded_window(incoming)
  bounded_window(outgoing)
  {
    next_out: next_outgoing,
    next_in: 0U,
    incoming,
    outgoing,
    peer_incoming: 0U,
    peer_outgoing: 0U,
    established: false,
  }
}

///|
pub fn SessionWindow::establish(
  self : SessionWindow,
  next_incoming : UInt32,
  peer_incoming : UInt32,
  peer_outgoing : UInt32,
) -> Unit raise AmqpError {
  if self.established {
    raise State("session already begun")
  }
  self.next_in = next_incoming
  self.peer_incoming = peer_incoming
  self.peer_outgoing = peer_outgoing
  self.established = true
}

///|
pub fn SessionWindow::can_send(
  self : SessionWindow,
  frames~ : Int32 = 1,
) -> Bool {
  frames >= 0 &&
  self.established &&
  frames.to_uint64() <= self.outgoing.to_uint64() &&
  frames.to_uint64() <= self.peer_incoming.to_uint64()
}

///|
pub fn SessionWindow::sent(
  self : SessionWindow,
  frames~ : Int32 = 1,
) -> Unit raise AmqpError {
  if !self.can_send(frames~) {
    raise State("session send window exhausted")
  }
  let count = frames.reinterpret_as_uint()
  self.next_out += count
  self.outgoing -= count
  self.peer_incoming -= count
}

///|
pub fn SessionWindow::received(self : SessionWindow) -> Unit raise AmqpError {
  if !self.established || self.incoming == 0U || self.peer_outgoing == 0U {
    raise State("session receive window exhausted")
  }
  self.next_in += 1U
  self.incoming -= 1U
  self.peer_outgoing -= 1U
}

///|
pub fn SessionWindow::update_peer(
  self : SessionWindow,
  next_incoming : UInt32?,
  incoming : UInt32,
  next_outgoing : UInt32,
  outgoing : UInt32,
) -> Unit raise AmqpError {
  if !self.established {
    raise State("session not begun")
  }
  if next_outgoing != self.next_in {
    raise Invalid("peer next-outgoing-id mismatch", 0)
  }
  let remaining = match next_incoming {
    Some(ack) => {
      let unacknowledged = serial_distance(ack, self.next_out)
      if unacknowledged > incoming {
        0U
      } else {
        incoming - unacknowledged
      }
    }
    None => raise Invalid("next-incoming-id required after begin", 0)
  }
  self.peer_incoming = remaining
  self.peer_outgoing = outgoing
}

///|
pub fn SessionWindow::refresh(
  self : SessionWindow,
  incoming : UInt32,
  outgoing : UInt32,
) -> Unit raise AmqpError {
  bounded_window(incoming)
  bounded_window(outgoing)
  self.incoming = incoming
  self.outgoing = outgoing
}

///|
pub fn SessionWindow::positions(self : SessionWindow) -> (UInt32, UInt32) {
  (self.next_in, self.next_out)
}

///|
pub fn SessionWindow::remaining(
  self : SessionWindow,
) -> (UInt32, UInt32, UInt32) {
  (self.incoming, self.outgoing, self.peer_incoming)
}

///|
test "session windows count transfer frames and correctly wrap uint32 IDs" {
  let session = SessionWindow::new(4U, 4U, next_outgoing=4294967295U)
  session.establish(9U, 2U, 3U)
  session.sent(frames=2)
  assert_eq(session.positions(), (9U, 1U))
  assert_false(session.can_send())
  session.update_peer(Some(1U), 2U, 9U, 3U)
  session.sent()
  session.received()
  assert_eq(session.positions(), (10U, 2U))
  assert_eq(serial_distance(4294967295U, 1U), 2U)
  assert_true(
    try {
      ignore(serial_distance(3U, 2U))
      false
    } catch {
      _ => true
    },
  )
}

///|
test "peer window UINT_MAX is a legal capacity rather than a serial distance" {
  let session = SessionWindow::new(16U, 16U)
  session.establish(0U, 4294967295U, 4294967295U)
  session.sent(frames=2)
  session.received()
  session.update_peer(Some(2U), 4294967295U, 1U, 4294967294U)
  assert_true(session.can_send())
  assert_eq(session.remaining().2, 4294967295U)
}
