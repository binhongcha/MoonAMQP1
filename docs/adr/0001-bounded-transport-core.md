# Separate an ordered-transport core from host services

The same AMQP 1.0 message and state logic must run in native, JavaScript and Wasm hosts, so the core consumes bytes/commands and exposes bytes/events instead of owning sockets. The first release supports one session with multiple links and receiver settlement first, trading transparent reconnection and transactions for inspectable bounded state; TLS, SASL, persistence, timers and retry policy remain explicit host responsibilities. AMQP 0-9-1 implementations cannot substitute for the different AMQP 1.0 type and link protocols.

The first-release session and settlement limits are superseded in the review branch by the [protocol profile](../PROTOCOL_PROFILE.md). The sans-I/O boundary remains the design decision.
