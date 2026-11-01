///|
test "expiry uses earlier TTL or absolute deadline and guards clock reversal" {
  let m = @moonamqp1.Message::new([
    { descriptor: 0x70, body: List([Null, Null, UInt(50U)]), },
    {
      descriptor: 0x73,
      body: List([
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        Null,
        Timestamp(130L),
      ]),
    },
    { descriptor: 0x75, body: Binary(b"job"), },
  ])
  assert_eq(m.expiry_status(100L, 110L).remaining_milliseconds, Some(20UL))
  assert_true(m.expiry_status(100L, 130L).expired)
  assert_true(
    try {
      ignore(m.expiry_status(100L, 99L))
      false
    } catch {
      _ => true
    },
  )
  assert_eq(
    @moonamqp1.Message::data(b"x").expiry_status(
      -9223372036854775808L, 9223372036854775807L,
    ).remaining_milliseconds,
    None,
  )
  let ttl = @moonamqp1.Message::new([
    { descriptor: 0x70, body: List([Null, Null, UInt(1U)]), },
  ])
  assert_true(
    ttl.expiry_status(-9223372036854775808L, 9223372036854775807L).expired,
  )
}
