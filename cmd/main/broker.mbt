///|
// The host adapter connects these streams to a dedicated local broker socket.
async fn broker(out_queue : String, in_queue : String, token : String) -> Unit {
  let endpoint = @amqp.Endpoint::new("moonamqp1-artemis-" + token, channel=3, limits={
    ..@amqp.Limits::default(),
    max_frame_bytes: 512,
  })
  let mut sent = false
  let mut accepted = false
  let mut received = 0U
  let mut drain_rounds = 0
  let mut closing = false
  endpoint.start()
  @stdio.stdout.write(endpoint.drain())
  while !endpoint.is_closed() {
    let bytes = match @stdio.stdin.read_some(max_len=16384) {
      Some(bytes) => bytes
      None => break
    }
    for byte in bytes {
      endpoint.receive(Bytes::from_array([byte]))
      for event in endpoint.events() {
        match event {
          RemoteOpen(_) => {
            endpoint.begin()
            endpoint.begin(channel=7)
          }
          SessionReady => ignore(endpoint.attach("moon-out", true, out_queue))
          SessionEvent(7, SessionReady) =>
            ignore(endpoint.attach("moon-in", false, in_queue, channel=7))
          Credit(handle, credit) if credit > 0U && !sent => {
            let body = Bytes::from_array(Array::make(5000, 0x42))
            let message = @amqp.Message::data(body, properties=[
              ("token", Text(token)),
              ("origin", Text("moonamqp1")),
            ])
            assert_true(endpoint.plan_send(handle, message, b"out").frames > 1)
            ignore(endpoint.send(handle, message, b"out"))
            sent = true
          }
          Settled(0U, Accepted) => accepted = true
          SessionEvent(7, LinkReady(handle, _, false)) =>
            endpoint.grant(handle, 1U, channel=7)
          SessionEvent(7, Received(delivery)) => {
            assert_true(received < 2U)
            assert_eq(delivery.id, received)
            assert_false(delivery.settled)
            let expected = if received == 0U {
              Bytes::from_array(Array::make(6000, 0xA5))
            } else {
              b"drain"
            }
            assert_eq(delivery.message.data_bytes(), expected)
            let mut matched_token = false
            for section in delivery.message.sections() {
              if section.descriptor == 0x74 && section.body is MapValue(entries) {
                for (key, value) in entries {
                  if key is Text("token") && value is Text(actual) {
                    matched_token = actual == token
                  }
                }
              }
            }
            assert_true(matched_token)
            endpoint.acknowledge(delivery.id, Accepted, channel=7)
            received += 1U
            if received == 1U {
              endpoint.grant(delivery.handle, 3U, drain=true, channel=7)
            }
          }
          SessionEvent(7, DrainCompleted(handle, unused)) => {
            assert_eq(received, 2U)
            assert_eq(unused, 2U)
            drain_rounds += 1
            if drain_rounds == 1 {
              endpoint.grant(handle, 2U, drain=true, channel=7)
            }
          }
          _ => ()
        }
      }
      if accepted && drain_rounds == 2 && !closing {
        endpoint.close()
        closing = true
      }
      let output = endpoint.drain()
      if !output.is_empty() {
        @stdio.stdout.write(output)
      }
    }
  }
  endpoint.finish_input()
  assert_true(accepted && received == 2U && drain_rounds == 2)
}
