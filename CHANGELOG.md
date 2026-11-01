# Changelog

## 0.1.0 — 2026-11-01

- 完整交付发送预检：Endpoint::plan_send
- 显式消息到期判断：Message::expiry_status
- 增加边界回归、可运行集成示例和CI步骤。

### Initial implementation

- Exact typed values, compound/array constructors, strict messages and incremental framing.
- Single-session multi-link sans-I/O connection, credit/windows, fragmentation, settlement and shutdown.
- Aggregate input/event budgets, atomic outgoing preflight and terminal incoming failure semantics.
- Runnable task, telemetry, bridge and bounded scale examples; independent Rhea wire and live-client verification.
- Public namespace: binhongcha/moonamqp1; reproducible compiler/core, Linux/Windows CI and history/package hygiene.
