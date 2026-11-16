///|
test "receiver-initiated reattachment validates the sender delivery count" {
  for reported in [1U, 2U] {
    let sender = Endpoint::new("sender")
    let receiver = Endpoint::new("receiver")
    sender.start()
    receiver.receive(sender.drain())
    sender.receive(receiver.drain())
    sender.begin()
    receiver.receive(sender.drain())
    sender.receive(receiver.drain())
    let send_handle = sender.attach("queue", true, "jobs")
    receiver.receive(sender.drain())
    sender.receive(receiver.drain())
    receiver.grant(receiver.primary.links[0].local_handle, 1U)
    sender.receive(receiver.drain())
    ignore(sender.send(send_handle, Message::data(b"job"), b"tag"))
    receiver.receive(sender.drain())
    receiver.acknowledge(0U, Accepted)
    sender.receive(receiver.drain())
    receiver.suspend_link(receiver.primary.links[0].local_handle)
    sender.receive(receiver.drain())
    receiver.receive(sender.drain())
    let handle = receiver.resume_link("queue")
    sender.receive(receiver.drain())
    let reply = sender.drain()
    if reported == 1U {
      receiver.receive(reply)
      assert_eq(receiver.link_credit(handle), 0U)
    } else {
      let fields = sender.primary.links[0].attach_fields(sender.limits)
      fields[9] = UInt(reported)
      sender.queue(sender.frame(0x12, fields, channel=sender.channel))
      assert_true(
        try {
          receiver.receive(sender.drain())
          false
        } catch {
          Invalid("resumed quiescent link counter differs", _) => true
          _ => false
        },
      )
    }
  }
}
