///|
fn main raise {
  let (sender, receiver) = @support.pair(frame_bytes=512, window=256U)
  let (outgoing, incoming) = @support.sender(
    sender, receiver, "batch", "jobs", 100U, 64U,
  )
  let body = Bytes::from_array(Array::makei(1000, i => (i % 251).to_byte()))
  let message = @amqp.Message::data(body, properties=[
    ("kind", Text("synthetic-scale")),
  ])
  let mut deliveries = 0
  let mut wire_bytes = 0
  let mut accepted = 0
  for batch = 0; batch < 32; batch = batch + 1 {
    for item = 0; item < 64; item = item + 1 {
      let id = sender.send(
        outgoing,
        message,
        @amqp.encode_value(UInt((batch * 64 + item).reinterpret_as_uint())),
      )
      assert_eq(id, deliveries.reinterpret_as_uint())
      deliveries += 1
    }
    let bytes = sender.drain()
    wire_bytes += bytes.length()
    receiver.receive(bytes)
    let events = receiver.events()
    assert_eq(events.length(), 64)
    for event in events {
      match event {
        Received(delivery) => {
          assert_eq(delivery.message.data_bytes(), body)
          receiver.acknowledge(delivery.id, Accepted)
        }
        _ => fail("batch delivery required")
      }
    }
    sender.receive(receiver.drain())
    for event in sender.events() {
      if event is Settled(_, Accepted) {
        accepted += 1
      }
    }
    assert_eq(sender.pending(), (0, 0))
    assert_eq(receiver.pending(), (0, 0))
    sender.refresh_windows(256U, 256U)
    receiver.receive(sender.drain())
    receiver.refresh_windows(256U, 256U)
    receiver.grant(incoming, 64U)
    sender.receive(receiver.drain())
    ignore(sender.events())
  }
  assert_eq(deliveries, 2048)
  assert_eq(accepted, deliveries)
  assert_eq(sender.session_positions().1, 6144U)
  @support.close(sender, receiver)
  println(
    "scale: \{deliveries} deliveries, 6144 transfer frames, \{wire_bytes} encoded bytes; all accepted with bounded 64-delivery batches",
  )
}
