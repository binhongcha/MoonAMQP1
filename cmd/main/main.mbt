///|
async fn peer() -> Unit {
  let endpoint = @amqp.Endpoint::new(
    "moonamqp1-stdio",
    channel=7,
    window=8192U,
    limits={ ..@amqp.Limits::default(), max_frame_bytes: 512, },
  )
  let sent : Map[UInt32, Bool] = Map([])
  // Binary stdin/stdout are an ordered host transport, with no SASL or TLS layer.
  while !endpoint.is_closed() {
    let bytes = match @stdio.stdin.read_some(max_len=16384) {
      Some(b) => b
      None => break
    }
    // Deliver a completed protocol item before applying host policy.
    for byte in bytes {
      endpoint.receive(Bytes::from_array([byte]))
      for event in endpoint.events() {
        match event {
          LinkReady(handle, _, false) => endpoint.grant(handle, 32U)
          Credit(handle, credit) =>
            if credit > 0U && !sent.contains(handle) {
              let message = @amqp.Message::data(
                Bytes::from_array(Array::make(5000, 0x42)),
                properties=[
                  ("origin", Text("moonamqp1")),
                  ("id", ULong(18446744073709551615UL)),
                ],
              )
              ignore(endpoint.send(handle, message, b"moon-delivery"))
              sent[handle] = true
            }
          Received(delivery) =>
            if !delivery.settled {
              endpoint.acknowledge(delivery.id, Accepted)
            }
          _ => ()
        }
      }
      let output = endpoint.drain()
      if !output.is_empty() {
        @stdio.stdout.write(output)
      }
    }
  }
  endpoint.finish_input()
}

///|
async fn run() -> Unit {
  let args = @env.args()
  if args.length() == 5 && args[1] == "broker" {
    broker(args[2], args[3], args[4])
    return
  }
  if args.length() == 2 && args[1] == "peer" {
    peer()
    return
  }
  if args.length() != 4 {
    raise Failure(
      "usage: moonamqp1 value|message|frames INPUT NEW_OUTPUT | peer | broker OUT_QUEUE IN_QUEUE TOKEN",
    )
  }
  if @fs.file_size(args[2][:]) >
    @amqp.Limits::default().max_value_bytes.to_int64() {
    raise Failure("input exceeds byte budget")
  }
  let bytes = @fs.read_file(args[2][:]).binary()
  let output = match args[1] {
    "value" => @amqp.encode_value(@amqp.decode_value(bytes))
    "message" => @amqp.encode_message(@amqp.decode_message(bytes))
    "frames" => {
      let stream = @amqp.FrameStream::new()
      let result : Array[Byte] = []
      for item in stream.feed(bytes) {
        let encoded = match item {
          Header => @amqp.protocol_header()
          AmqpFrame(frame) => @amqp.encode_frame(frame)
        }
        for byte in encoded {
          result.push(byte)
        }
      }
      stream.finish()
      Bytes::from_array(result)
    }
    _ => raise Failure("unknown codec command")
  }
  @fs.write_file(args[3][:], output, create_mode=CreateNew)
}

///|
async fn main {
  run() catch {
    @amqp.AmqpError::Invalid(message, offset) =>
      raise Failure("invalid AMQP at byte \{offset}: \{message}")
    @amqp.AmqpError::Unsupported(message) =>
      raise Failure("unsupported AMQP profile: \{message}")
    @amqp.AmqpError::Limit(message) =>
      raise Failure("AMQP resource limit: \{message}")
    @amqp.AmqpError::State(message) =>
      raise Failure("AMQP state error: \{message}")
    error => raise error
  }
}
