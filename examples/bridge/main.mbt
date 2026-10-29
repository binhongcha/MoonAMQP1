///|
fn main raise {
  let (producer, ingress) = @support.pair(frame_bytes=512)
  let (egress, consumer) = @support.pair(frame_bytes=1024)
  let (upstream, _) = @support.sender(
    producer, ingress, "upstream", "events/in", 21U, 1U,
  )
  let (downstream, _) = @support.sender(
    egress, consumer, "downstream", "events/out", 31U, 1U,
  )
  let original = @amqp.Message::new([
    { descriptor: 0x70, body: List([Boolean(true), UByte(8), UInt(60000U)]), },
    {
      descriptor: 0x72,
      body: MapValue([
        (Symbol("x-trace"), Text("original-trace")),
        (ULong(1024UL), Binary(b"vendor-data")),
      ]),
    },
    {
      descriptor: 0x73,
      body: List([ULong(18446744073709551615UL), Null, Text("events/out")]),
    },
    { descriptor: 0x74, body: MapValue([(Text("schema"), Text("asset.v1"))]), },
    { descriptor: 0x75, body: Binary(b"part-one"), },
    { descriptor: 0x75, body: Binary(b""), },
    { descriptor: 0x75, body: Binary(b"part-two"), },
    {
      descriptor: 0x78,
      body: MapValue([(Symbol("checksum-kind"), Symbol("host-managed"))]),
    },
  ])
  let original_bytes = @amqp.encode_message(original)
  let upstream_id = producer.send(upstream, original, b"external-id")
  ingress.receive(producer.drain())
  let inbound = match ingress.events()[0] {
    Received(delivery) => delivery
    _ => fail("ingress delivery expected")
  }
  assert_eq(@amqp.encode_message(inbound.message), original_bytes)
  // Link tags and delivery IDs belong to each hop, not to message identity.
  let downstream_id = egress.send(
    downstream,
    inbound.message,
    b"bridge-attempt-1",
  )
  consumer.receive(egress.drain())
  let delivered = match consumer.events()[0] {
    Received(delivery) => delivery
    _ => fail("egress delivery expected")
  }
  assert_eq(delivered.message.sections().length(), 8)
  assert_eq(delivered.message.data_bytes(), b"part-onepart-two")
  assert_eq(@amqp.encode_message(delivered.message), original_bytes)
  assert_eq(delivered.tag, b"bridge-attempt-1")
  consumer.acknowledge(downstream_id, Accepted)
  egress.receive(consumer.drain())
  assert_true(egress.events()[0] is Settled(_, Accepted))
  // Host policy acknowledges upstream only after downstream acceptance.
  // This demonstration has no durable outbox and makes no exactly-once claim.
  ingress.acknowledge(upstream_id, Accepted)
  producer.receive(ingress.drain())
  assert_true(producer.events()[0] is Settled(_, Accepted))
  @support.close(producer, ingress)
  @support.close(egress, consumer)
  println(
    "bridge: two independently negotiated sessions retain eight typed sections; upstream confirmation follows downstream acceptance",
  )
}
