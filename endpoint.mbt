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
  LinkSuspended(String, UInt32)
  RecoverySettled(UInt32, Bytes)
  DeliveryRebound(UInt32, UInt32, Bytes)
  RecoveryStoreFull(String)
  SessionEnded(Value)
  RemoteClosed(Value)
  SessionEvent(Int32, Event)
  DrainRequested(UInt32, UInt32)
  DrainCompleted(UInt32, UInt32)
}

///|
pub struct Endpoint {
  priv limits : Limits
  priv container : String
  priv channel : Int32
  priv default_window : UInt32
  priv max_sessions : Int32
  priv mut primary : SessionState
  priv sessions : Map[Int32, SessionState]
  priv remote_sessions : Map[Int32, SessionState]
  priv stream : FrameStream
  priv io : EndpointBuffers
  priv link_bindings : Map[(String, String, String, Bool), LinkBinding]
  priv mut started : Bool
  priv mut remote_open : Bool
  priv mut remote_container : String?
  priv recovery_store : RecoveryStore?
  priv idle_advertised_ms : UInt32
  priv mut peer_idle_ms : UInt32
  priv mut clock_ms : UInt64?
  priv mut last_rx_ms : UInt64
  priv mut last_tx_ms : UInt64
  priv mut peer_frame_max : Int32
  priv mut peer_channel_max : Int32
  priv mut closed : Bool
  priv mut failed : Bool
}

///|
pub fn Endpoint::new(
  container : String,
  channel~ : Int32 = 0,
  window~ : UInt32 = 1024U,
  max_sessions~ : Int32 = 16,
  idle_advertised_ms~ : UInt32 = 0U,
  recovery~ : RecoveryStore? = None,
  limits~ : Limits = Limits::default(),
) -> Endpoint raise AmqpError {
  limits.validate()
  require_range(channel, 0, 65535)
  require_range(max_sessions, 1, 64)
  if text_bytes(container, maximum=4096).length() == 0 {
    raise Invalid("container-id length", 0)
  }
  {
    limits,
    container,
    channel,
    default_window: window,
    max_sessions,
    primary: session_state(channel, window),
    sessions: Map([]),
    remote_sessions: Map([]),
    stream: FrameStream::new(limits~),
    io: {
      limits,
      channel,
      outbound: [],
      events: [],
      queued_bytes: 0,
      partial_bytes: 0,
      event_bytes: 0,
      closing: false,
    },
    link_bindings: recovery
    .map(store => store.bindings)
    .unwrap_or_else(() => Map([])),
    started: false,
    remote_open: false,
    remote_container: None,
    recovery_store: recovery,
    idle_advertised_ms,
    peer_idle_ms: 0U,
    clock_ms: None,
    last_rx_ms: 0UL,
    last_tx_ms: 0UL,
    peer_frame_max: 512,
    peer_channel_max: 65535,
    closed: false,
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
fn Endpoint::session_active(
  self : Endpoint,
  channel : Int32,
) -> SessionState raise AmqpError {
  self.active()
  if self.io.closing {
    raise State("connection close pending")
  }
  let session = match self.sessions.get(channel) {
    Some(session) => session
    None => raise State("unknown local session channel")
  }
  if session.end_sent {
    raise State("session end pending or complete")
  }
  if session.remote_channel is None {
    raise State("session not active")
  }
  session
}

///|
fn Endpoint::queue(self : Endpoint, bytes : Bytes) -> Unit raise AmqpError {
  self.io.check_queue(bytes.length(), 1)
  self.io.append(bytes)
}

///|
fn Endpoint::frame(
  self : Endpoint,
  code : Int32,
  fields : Array[Value],
  payload~ : Bytes = b"",
  channel~ : Int32 = self.channel,
) -> Bytes raise AmqpError {
  encode_frame(
    {
      channel: if code == 0x10 || code == 0x18 {
        0
      } else {
        channel
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
  if self.io.events.length() >= self.limits.max_pending {
    raise Limit("event queue; drain events")
  }
  self.io.events.push(event)
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
      if self.idle_advertised_ms == 0U {
        Null
      } else {
        UInt(self.idle_advertised_ms)
      },
    ])
  self.queue(bytes)
  self.started = true
}

///|
pub fn Endpoint::drain(self : Endpoint) -> Bytes {
  let output = wire_writer(self.io.queued_bytes)
  for bytes in self.io.outbound {
    output.put(bytes) catch {
      _ => abort("queue invariant")
    }
  }
  self.io.outbound.clear()
  self.io.queued_bytes = 0
  output.finish()
}

///|
pub fn Endpoint::events(self : Endpoint) -> Array[Event] {
  let events = self.io.events.copy()
  self.io.events.clear()
  self.io.event_bytes = 0
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
fn SessionState::local_link(
  self : SessionState,
  handle : UInt32,
) -> LinkState raise AmqpError {
  for link in self.links {
    if link.local_handle == handle &&
      !link.closed &&
      !link.suspended &&
      !link.pause_pending {
      return link
    }
  }
  raise State("unknown local link handle")
}

///|
fn SessionState::remote_link(
  self : SessionState,
  handle : UInt32,
) -> LinkState raise AmqpError {
  for link in self.links {
    if link.remote_handle == Some(handle) && !link.closed {
      return link
    }
  }
  raise Invalid("unknown remote link handle", 0)
}

///|
priv struct SessionState {
  channel : Int32
  window : SessionWindow
  links : Array[LinkState]
  outgoing : Map[UInt32, UInt32]
  incoming : Map[UInt32, UInt32]
  outgoing_tags : Map[UInt32, Bytes]
  incoming_tags : Map[UInt32, Bytes]
  recovering_incoming : Map[UInt32, (LinkState, Bytes)]
  recovering_outgoing : Map[UInt32, (LinkState, Bytes)]
  incoming_first : Set[UInt32]
  reported : Map[UInt32, Outcome]
  mut end_sent : Bool
  mut remote_channel : Int32?
  mut peer_handle_max : UInt32
  mut next_delivery : UInt32
  mut remote_next_delivery : UInt32?
}

///|
fn session_state(
  channel : Int32,
  window : UInt32,
) -> SessionState raise AmqpError {
  {
    channel,
    window: SessionWindow::new(window, window),
    links: [],
    outgoing: Map([]),
    incoming: Map([]),
    outgoing_tags: Map([]),
    incoming_tags: Map([]),
    recovering_incoming: Map([]),
    recovering_outgoing: Map([]),
    incoming_first: Set([]),
    reported: Map([]),
    end_sent: false,
    remote_channel: None,
    peer_handle_max: 4294967295U,
    next_delivery: 0U,
    remote_next_delivery: None,
  }
}

///|
fn Endpoint::session_event(
  self : Endpoint,
  session : SessionState,
  event : Event,
) -> Unit raise AmqpError {
  self.io.session_event(session, event)
}
