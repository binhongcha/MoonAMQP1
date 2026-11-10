///|
fn main raise {
  let (producer, worker) = @support.pair()
  producer.begin(channel=9)
  worker.receive(producer.drain())
  producer.receive(worker.drain())
  let worker_channel = worker.session_diagnostics()[0].channel
  let handle = producer.attach(
    "job-batch",
    true,
    "jobs",
    channel=9,
    receiver_mode=Second,
  )
  worker.receive(producer.drain())
  producer.receive(worker.drain())
  worker.grant(0U, 3U, drain=true, channel=worker_channel)
  producer.receive(worker.drain())
  let job = @amqp.Message::data(b"job")
  let first = producer.send(handle, job, b"job-1", channel=9)
  let last = producer.send(handle, job, b"job-2", channel=9)
  producer.complete_drain(handle, channel=9)
  worker.receive(producer.drain())
  assert_true(
    worker
    .events()
    .any(event => event is SessionEvent(_, DrainCompleted(_, 1U))),
  )
  worker.acknowledge_range(first, last, Accepted, channel=worker_channel)
  assert_eq(worker.session_diagnostics()[0].awaiting_sender_confirmation, 2)
  producer.receive(worker.drain())
  worker.receive(producer.drain())
  assert_eq(worker.session_diagnostics()[0].awaiting_sender_confirmation, 0)
  assert_eq(producer.session_diagnostics().length(), 2)
  @support.close(producer, worker)
  println(
    "Two sessions, 2 jobs confirmed by one range, 1 unused credit returned",
  )
}
