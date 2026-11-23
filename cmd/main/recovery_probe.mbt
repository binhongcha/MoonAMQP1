///|
// Drives the public core against the independent recovery fixture over TCP.
async fn probe_recovery(port : String, sender : Bool) -> Unit {
  let address = @socket.Addr::parse("127.0.0.1:" + port)
  let store = @amqp.RecoveryStore::new()
  recovery_probe_connection(address, store, sender, false)
  assert_eq(store.retained_links(), 1)
  recovery_probe_connection(address, store, sender, true)
}

///|
async fn recovery_probe_connection(
  address : @socket.Addr,
  store : @amqp.RecoveryStore,
  sender : Bool,
  resumed : Bool,
) -> Unit {
  let socket = @socket.Tcp::connect(address)
  defer socket.close()
  let name = "tasks"
  let tag_a = b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  let tag_b = b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  let tag_c = b"new"
  let limits = { ..@amqp.Limits::default(), max_frame_bytes: 620, }
  let end = @amqp.Endpoint::new(
    "moon-recovery-probe",
    channel=3,
    limits~,
    recovery=Some(store),
  )
  let body = Bytes::from_array(Array::make(1400, b'x'))
  let mut sent = false
  let mut recovered = false
  let mut sender_b : UInt32? = None
  let mut received = 0
  let mut handle : UInt32? = None
  end.start()
  socket.write(end.drain())
  while !end.is_closed() {
    guard socket.read_some(max_len=4096) is Some(wire) else { break }
    end.receive(wire)
    for event in end.events() {
      match event {
        RemoteOpen(_) => end.begin()
        SessionReady =>
          if resumed {
            handle = Some(end.resume_link(name, handle=11U))
          } else {
            handle = Some(
              end.attach(
                name,
                sender,
                String::make(194, 'x'),
                handle=4U,
                receiver_mode=Second,
                recoverable=true,
              ),
            )
          }
        LinkReady(h, _, _) => {
          handle = Some(h)
          if !sender {
            end.grant(h, 4U)
          }
        }
        Credit(h, credit) if sender && credit > 0U =>
          if !resumed && !sent {
            assert_true(credit >= 2U)
            ignore(end.send(h, @amqp.Message::data(b"done"), tag_a))
            ignore(end.send(h, @amqp.Message::data(body), tag_b))
            sent = true
          } else if resumed {
            let tags = end.resumable_deliveries(h)
            if tags is [tag] {
              let id = end.resume_delivery(h, tag)
              if tag == tag_a {
                assert_true(end.recovery_status(h).peer_incomplete)
                end.suspend_link(h)
              } else {
                assert_eq(tag, tag_b)
                assert_false(end.recovery_status(h).peer_incomplete)
                sender_b = Some(id)
              }
            }
          }
        LinkSuspended(_, _) if sender && resumed =>
          handle = Some(end.resume_link(name, handle=12U))
        Settled(id, Accepted) if sender_b == Some(id) => {
          assert_eq(end.pending(), (0, 0))
          ignore(
            end.send(
              handle.unwrap(),
              @amqp.Message::data(b"next"),
              tag_c,
              settled=true,
            ),
          )
          recovered = true
          end.close()
        }
        Received(delivery) if !sender => {
          received += 1
          if !resumed {
            assert_eq(delivery.tag, tag_a)
            assert_eq(delivery.message.data_bytes(), b"done")
            end.acknowledge(delivery.id, Accepted)
          } else if delivery.tag == tag_b {
            assert_eq(delivery.message.data_bytes(), body)
            assert_eq(delivery.id, 1U)
            end.acknowledge(delivery.id, Accepted)
          } else {
            assert_eq(delivery.tag, tag_c)
            assert_eq(delivery.message.data_bytes(), b"next")
            assert_eq(end.pending(), (0, 0))
            recovered = true
            end.close()
          }
        }
        _ => ()
      }
    }
    let bytes = end.drain()
    if !bytes.is_empty() {
      socket.write(bytes)
    }
  }
  if resumed {
    assert_true(recovered && end.is_closed())
    if !sender {
      assert_eq(received, 2)
    }
  } else {
    assert_false(end.is_closed())
    if sender {
      assert_true(sent)
    } else {
      assert_eq(received, 1)
    }
    end.transport_lost()
  }
}
