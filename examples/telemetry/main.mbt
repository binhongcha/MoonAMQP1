///|
fn main raise {
  let (edge, service) = @support.pair(frame_bytes=1024)
  let (slow, _) = @support.sender(
    edge, service, "bulk-history", "sensors/history", 50U, 0U,
  )
  let (live, _) = @support.sender(
    edge, service, "live-samples", "sensors/live", 51U, 2U,
  )
  let (alerts, _) = @support.sender(
    edge, service, "alarm-events", "sensors/alarms", 52U, 1U,
  )
  let exact_id = 18446744073709551615UL
  let message = @amqp.Message::new([
    {
      descriptor: 0x73,
      body: List([
        ULong(exact_id),
        Null,
        Text("sensors/live"),
        Text("temperature"),
        Null,
        Null,
        Symbol("application/octet-stream"),
        Null,
        Null,
        Timestamp(1793232000123L),
      ]),
    },
    {
      descriptor: 0x74,
      body: MapValue([
        (Text("device"), ULong(exact_id)),
        (Text("unit"), Text("C")),
        (Text("quality"), UByte(100)),
      ]),
    },
    { descriptor: 0x75, body: Binary(b"\x00\x41\xa0\x00"), },
  ])
  assert_true(
    try {
      ignore(edge.send(slow, message, b"history"))
      false
    } catch {
      _ => true
    },
  )
  ignore(edge.send(live, message, b"sample-1"))
  ignore(edge.send(live, message, b"sample-2"))
  ignore(
    edge.send(
      alerts,
      @amqp.Message::data(b"threshold", properties=[("device", ULong(exact_id))]),
      b"alarm",
    ),
  )
  assert_eq(edge.link_credit(slow), 0U)
  assert_eq(edge.link_credit(live), 0U)
  service.receive(edge.drain())
  let events = service.events()
  assert_eq(events.length(), 3)
  let mut samples = 0
  for event in events {
    match event {
      Received(delivery) => {
        if delivery.tag != b"alarm" {
          samples += 1
          let properties = delivery.message.sections()[0]
          match properties.body {
            List(fields) => {
              assert_eq(
                @amqp.encode_value(fields[0]),
                b"\x80\xff\xff\xff\xff\xff\xff\xff\xff",
              )
              assert_true(fields[9] is Timestamp(1793232000123L))
            }
            _ => fail("typed properties required")
          }
        }
        service.acknowledge(delivery.id, Accepted)
      }
      _ => fail("only complete telemetry expected")
    }
  }
  edge.receive(service.drain())
  assert_eq(edge.events().length(), 3)
  assert_eq(samples, 2)
  assert_eq(edge.pending(), (0, 0))
  @support.close(edge, service)
  println(
    "telemetry: blocked history link, live samples and alarm link share a session; uint64 IDs and timestamps retained",
  )
}
