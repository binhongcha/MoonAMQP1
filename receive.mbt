///|
fn Endpoint::receive_transfer(
  self : Endpoint,
  session : SessionState,
  perf : Performative,
  payload : Bytes,
) -> Unit raise AmqpError {
  let link = session.remote_link(uint_field(perf, 0, 0U))
  if link.sender || (!link.ready && !link.pause_pending) {
    raise State("transfer requires attached receiver")
  }
  if uint_field(perf, 3, 0U) != 0U {
    raise Unsupported("nonzero message-format")
  }
  let resuming = bool_field(perf, 8, false)
  let aborted = bool_field(perf, 9, false)
  let more = !aborted && bool_field(perf, 5, false)
  if (link.sent_incomplete || link.peer_incomplete) &&
    link.partial is None &&
    !resuming {
    session.window.received()
    self.end_link_with_error(
      session, link, "amqp:illegal-state", "new delivery while unsettled map is incomplete",
    )
    return
  }
  let partial = match link.partial {
    Some(partial) => {
      match perf.field(1) {
        Null => ()
        UInt(n) if n == partial.id => ()
        _ => raise Invalid("continuation delivery-id", 0)
      }
      match perf.field(2) {
        Null => ()
        Binary(tag) if tag == partial.tag => ()
        _ => raise Invalid("continuation delivery-tag", 0)
      }
      match perf.field(4) {
        Null => ()
        Boolean(n) if n == partial.settled => ()
        _ => raise Unsupported("settlement change during fragmented delivery")
      }
      if !partial.settled {
        match perf.field(6) {
          Null => ()
          UByte(0) if partial.receiver_mode == First => ()
          UByte(1) if partial.receiver_mode == Second => ()
          _ =>
            raise Unsupported(
              "receiver settlement mode changed during delivery",
            )
        }
      }
      partial
    }
    None => {
      if link.credit == 0U {
        session.window.received()
        self.end_link_with_error(
          session, link, "amqp:link:transfer-limit-exceeded", "remote transfer exceeded link credit",
        )
        return
      }
      let id = match perf.field(1) {
        UInt(n) => n
        _ => raise Invalid("first transfer requires delivery-id", 0)
      }
      let tag = match perf.field(2) {
        Binary(b) => b
        _ => raise Invalid("first transfer requires delivery-tag", 0)
      }
      match session.remote_next_delivery {
        Some(expected) if id != expected =>
          raise Invalid("non-sequential session delivery-id", 0)
        _ => ()
      }
      let settled = bool_field(perf, 4, false)
      let record = if resuming { link.records.get(tag) } else { None }
      let mode = match perf.field(6) {
        Null => link.receiver_mode
        UByte(0) => First
        UByte(1) if link.receiver_mode == Second => Second
        _ => raise Invalid("transfer receiver settle mode exceeds link mode", 0)
      }
      if session.incoming.contains(id) {
        raise Invalid("delivery-id wrap collides with unsettled delivery", 0)
      }
      if (link.settle_mode == 0 && settled && !aborted) ||
        (link.settle_mode == 1 && !settled && !aborted) {
        raise Invalid(
          "transfer settlement disagrees with sender settle mode", 0,
        )
      }
      if !settled &&
        (
          session.incoming.contains(id) ||
          session.incoming.length() >= self.limits.max_pending ||
          (record is None && link.records.length() >= self.limits.max_pending)
        ) {
        raise Limit("incoming unsettled deliveries")
      }
      for (pending_id, pending_handle) in session.incoming {
        if pending_handle == link.local_handle &&
          session.incoming_tags[pending_id] == tag {
          raise Invalid("duplicate unsettled delivery-tag on receiver link", 0)
        }
      }
      if !resuming && link.records.contains(tag) {
        raise Invalid("duplicate retained delivery-tag on receiver link", 0)
      }
      let recovered_terminal = if resuming &&
        !aborted &&
        !more &&
        record is Some(_) {
        terminal_state(perf.field(7))
      } else {
        None
      }
      let receiver_mode = if settled { First } else { mode }
      if recovered_terminal is Some(outcome) && record is Some(entry) {
        let previous = terminal_state(entry.state)
        let settle_response = mode == First ||
          (previous is Some(_) && previous != Some(outcome))
        if !settled {
          self.queue(
            self.disposition_frame(
              session,
              true,
              id,
              id,
              settle_response,
              outcome.value(),
            ),
          )
        }
        session.window.received()
        link.credit -= 1U
        link.delivery_count += 1U
        session.remote_next_delivery = Some(id + 1U)
        if settled || settle_response {
          link.records.remove(tag) |> ignore
          self.session_event(session, PeerSettled(id))
        } else {
          entry.id = Some(id)
          entry.state = outcome.value()
          entry.bytes = b""
          session.incoming[id] = link.local_handle
          session.incoming_tags[id] = tag
          session.reported[id] = outcome
          session.recovering_incoming[id] = (link, tag)
        }
        self.complete_received_drain(session, link)
        return
      }
      let prefix = match record {
        Some(entry) if !aborted => entry.bytes
        _ => b""
      }
      let skip = if record is Some(_) && !aborted {
        let start = match received_fields(perf.field(7)) {
          Some((section, offset)) =>
            recovery_offset(prefix, section, offset, self.limits)
          None => 0
        }
        prefix.length() - start
      } else {
        0
      }
      if prefix.length() >
        self.limits.max_value_bytes -
        self.io.partial_bytes -
        self.io.event_bytes {
        raise Limit("restored incoming prefix budget")
      }
      let partial : PartialDelivery = {
        id,
        tag,
        settled,
        receiver_mode,
        bytes: prefix.to_array(),
        fragments: 0,
        skip,
        ignored: resuming && record is None,
      }
      self.io.partial_bytes += prefix.length()
      if record is Some(entry) {
        // The prefix is now owned by the transfer assembler.
        entry.bytes = b""
      }
      link.credit -= 1U
      link.delivery_count += 1U
      session.remote_next_delivery = Some(id + 1U)
      link.partial = Some(partial)
      if !settled && !partial.ignored {
        session.incoming[id] = link.local_handle
        session.incoming_tags[id] = tag
        if receiver_mode == First {
          session.incoming_first.add(id)
        }
        if link.recoverable {
          let entry = record.unwrap_or_else(() => {
            id: None,
            state: Null,
            bytes: b"",
            delivered: false,
          })
          entry.id = Some(id)
          link.records[tag] = entry
          session.recovering_incoming[id] = (link, tag)
        }
      }
      partial
    }
  }
  session.window.received()
  if partial.ignored {
    if !more {
      link.partial = None
      self.complete_received_drain(session, link)
    }
    return
  }
  let skipped = minimum(partial.skip, payload.length())
  partial.skip -= skipped
  let remaining = payload.length() - skipped
  if !aborted &&
    remaining > self.limits.max_message_bytes - partial.bytes.length() {
    self.end_link_with_error(
      session, link, "amqp:link:message-size-exceeded", "incoming message exceeds negotiated limit",
    )
    return
  }
  if partial.fragments >= self.limits.max_fragments {
    raise Limit("incoming fragment count")
  }
  partial.fragments += 1
  if aborted {
    session.forget_incoming(partial.id)
    link.records.remove(partial.tag) |> ignore
    self.io.partial_bytes -= partial.bytes.length()
    link.partial = None
    self.session_event(session, Aborted(link.local_handle, partial.id))
    self.complete_received_drain(session, link)
    return
  }
  if remaining >
    self.limits.max_value_bytes - self.io.partial_bytes - self.io.event_bytes {
    raise Limit("aggregate incoming payload budget; drain events")
  }
  for byte in payload[skipped:] {
    partial.bytes.push(byte)
  }
  self.io.partial_bytes += remaining
  if !more {
    if partial.skip != 0 {
      raise Invalid("resumed payload did not reach retained offset", 0)
    }
    let wire = Bytes::from_array(partial.bytes)
    let message = decode_message(wire, limits=self.limits)
    link.partial = None
    self.io.partial_bytes -= partial.bytes.length()
    let mut publish = true
    if link.records.get(partial.tag) is Some(entry) {
      if partial.settled {
        link.records.remove(partial.tag) |> ignore
      } else {
        self.check_retained_bytes(wire.length())
        entry.bytes = wire
        entry.state = prefix_state(wire, self.limits)
      }
      if entry.delivered {
        publish = false
        self.session_event(
          session,
          DeliveryRebound(link.local_handle, partial.id, partial.tag),
        )
      }
      entry.delivered = true
    }
    if publish {
      self.io.event_bytes += partial.bytes.length()
      self.session_event(
        session,
        Received({
          handle: link.local_handle,
          id: partial.id,
          tag: partial.tag,
          settled: partial.settled,
          message,
        }),
      )
    }
    self.complete_received_drain(session, link)
  }
}
