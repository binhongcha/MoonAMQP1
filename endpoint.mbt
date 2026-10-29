///|
pub(all) struct Delivery {
  handle : UInt32
  id : UInt32
  tag : Bytes
  settled : Bool
  message : Message
}

///|
pub enum Event {
  RemoteOpen(String)
  SessionReady
  LinkReady(UInt32, String, Bool)
  Credit(UInt32, UInt32)
  Received(Delivery)
  Settled(UInt32, Outcome)
  PeerSettled(UInt32)
  UnsettledLost(UInt32, Bool)
  Aborted(UInt32, UInt32)
  Detached(UInt32)
  SessionEnded(Value)
  RemoteClosed(Value)
}

///|
pub struct Endpoint {
  priv limits : Limits
  priv container : String
  priv channel : Int32
  priv stream : FrameStream
  priv window : SessionWindow
  priv links : Array[LinkState]
  priv outbound : Array[Bytes]
  priv events : Array[Event]
  priv outgoing : Map[UInt32, UInt32]
  priv incoming : Map[UInt32, UInt32]
  priv outgoing_tags : Map[UInt32, Bytes]
  priv incoming_tags : Map[UInt32, Bytes]
  priv mut queued_bytes : Int32
  priv mut partial_bytes : Int32
  priv mut event_bytes : Int32
  priv mut started : Bool
  priv mut remote_open : Bool
  priv mut begin_sent : Bool
  priv mut end_sent : Bool
  priv mut remote_channel : Int32?
  priv mut peer_frame_max : Int32
  priv mut peer_handle_max : UInt32
  priv mut peer_channel_max : Int32
  priv mut next_delivery : UInt32
  priv mut remote_next_delivery : UInt32?
  priv mut closed : Bool
  priv mut closing : Bool
  priv mut failed : Bool
}

///|
pub fn Endpoint::new(
  container : String,
  channel~ : Int32 = 0,
  window~ : UInt32 = 1024U,
  limits~ : Limits = Limits::default(),
) -> Endpoint raise AmqpError {
  limits.validate()
  require_range(channel, 0, 65535)
  if text_bytes(container, maximum=4096).length() == 0 {
    raise Invalid("container-id length", 0)
  }
  {
    limits,
    container,
    channel,
    stream: FrameStream::new(limits~),
    window: SessionWindow::new(window, window),
    links: [],
    outbound: [],
    events: [],
    outgoing: Map([]),
    incoming: Map([]),
    outgoing_tags: Map([]),
    incoming_tags: Map([]),
    queued_bytes: 0,
    partial_bytes: 0,
    event_bytes: 0,
    started: false,
    remote_open: false,
    begin_sent: false,
    end_sent: false,
    remote_channel: None,
    peer_frame_max: 512,
    peer_handle_max: 4294967295U,
    peer_channel_max: 65535,
    next_delivery: 0U,
    remote_next_delivery: None,
    closed: false,
    closing: false,
    failed: false,
  }
}

///|
fn Endpoint::active(self : Endpoint) -> Unit raise AmqpError {
  if self.failed || self.closed {
    raise State("endpoint closed or failed")
  }
}

///|
fn Endpoint::session_active(self : Endpoint) -> Unit raise AmqpError {
  self.active()
  if self.closing {
    raise State("connection close pending")
  }
  if self.end_sent {
    raise State("session end pending or complete")
  }
  if self.remote_channel is None {
    raise State("session not active")
  }
}

///|
fn Endpoint::queue(self : Endpoint, bytes : Bytes) -> Unit raise AmqpError {
  if bytes.length() > self.limits.max_value_bytes - self.queued_bytes ||
    self.outbound.length() >= self.limits.max_items {
    raise Limit("outbound queue; drain before more commands")
  }
  self.outbound.push(bytes)
  self.queued_bytes += bytes.length()
}

///|
fn Endpoint::frame(
  self : Endpoint,
  code : Int32,
  fields : Array[Value],
  payload~ : Bytes = b"",
) -> Bytes raise AmqpError {
  encode_frame(
    {
      channel: if code == 0x10 || code == 0x18 {
        0
      } else {
        self.channel
      },
      performative: Some(Performative::new(code, fields, limits=self.limits)),
      payload,
      extended_header: b"",
    },
    limits={
      ..self.limits,
      max_frame_bytes: minimum(self.limits.max_frame_bytes, self.peer_frame_max),
    },
  )
}

///|
fn Endpoint::event(self : Endpoint, event : Event) -> Unit raise AmqpError {
  if self.events.length() >= self.limits.max_pending {
    raise Limit("event queue; drain events")
  }
  self.events.push(event)
}

///|
/// Start both the protocol header and local open. No network I/O occurs here.
pub fn Endpoint::start(self : Endpoint) -> Unit raise AmqpError {
  self.active()
  if self.started {
    raise State("endpoint already started")
  }
  let bytes = protocol_header() +
    self.frame(0x10, [
      Text(self.container),
      Null,
      UInt(self.limits.max_frame_bytes.reinterpret_as_uint()),
      UShort(65535),
    ])
  self.queue(bytes)
  self.started = true
}

///|
pub fn Endpoint::drain(self : Endpoint) -> Bytes {
  let output = wire_writer(self.queued_bytes)
  for bytes in self.outbound {
    output.put(bytes) catch {
      _ => abort("queue invariant")
    }
  }
  self.outbound.clear()
  self.queued_bytes = 0
  output.finish()
}

///|
pub fn Endpoint::events(self : Endpoint) -> Array[Event] {
  let events = self.events.copy()
  self.events.clear()
  self.event_bytes = 0
  events
}

///|
pub fn Endpoint::is_failed(self : Endpoint) -> Bool {
  self.failed
}

///|
pub fn Endpoint::is_closed(self : Endpoint) -> Bool {
  self.closed
}

///|
fn Endpoint::local_link(
  self : Endpoint,
  handle : UInt32,
) -> LinkState raise AmqpError {
  for link in self.links {
    if link.local_handle == handle && !link.closed {
      return link
    }
  }
  raise State("unknown local link handle")
}

///|
fn Endpoint::remote_link(
  self : Endpoint,
  handle : UInt32,
) -> LinkState raise AmqpError {
  for link in self.links {
    if link.remote_handle == Some(handle) && !link.closed {
      return link
    }
  }
  raise Invalid("unknown remote link handle", 0)
}
