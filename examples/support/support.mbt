///|
pub fn pair(
  frame_bytes~ : Int32 = 512,
  window~ : UInt32 = 256U,
) -> (@amqp.Endpoint, @amqp.Endpoint) raise {
  let limits = { ..@amqp.Limits::default(), max_frame_bytes: frame_bytes, }
  let a = @amqp.Endpoint::new("application", channel=3, window~, limits~)
  let b = @amqp.Endpoint::new("service", channel=17, window~, limits~)
  a.start()
  b.receive(a.drain())
  a.receive(b.drain())
  a.begin()
  b.receive(a.drain())
  a.receive(b.drain())
  ignore(a.events())
  ignore(b.events())
  (a, b)
}

///|
pub fn sender(
  a : @amqp.Endpoint,
  b : @amqp.Endpoint,
  name : String,
  address : String,
  handle : UInt32,
  credit : UInt32,
) -> (UInt32, UInt32) raise {
  let outgoing = a.attach(name, true, address, handle~)
  b.receive(a.drain())
  a.receive(b.drain())
  let mut incoming : UInt32? = None
  for event in b.events() {
    if event is LinkReady(handle, _, false) {
      incoming = Some(handle)
    }
  }
  let incoming = incoming.unwrap()
  b.grant(incoming, credit)
  a.receive(b.drain())
  ignore(a.events())
  (outgoing, incoming)
}

///|
pub fn close(a : @amqp.Endpoint, b : @amqp.Endpoint) -> Unit raise {
  a.close()
  b.receive(a.drain())
  a.receive(b.drain())
  assert_true(a.is_closed())
  assert_true(b.is_closed())
  a.finish_input()
  b.finish_input()
}
