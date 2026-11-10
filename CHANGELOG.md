# Changelog

## 0.2.1 — 2026-11-10

- 修正复审提交的作者归属并补发版本。协议实现和公开 API 与 0.2.0 一致。

## 0.2.0 — 2026-11-10

- 同一连接支持多 Session，独立映射 channel、窗口与 delivery ID。
- 增加 Receiver Settle Mode Second 及 link drain 两种角色的状态处理。
- 新增批量结果确认和会话流控快照，提供可执行复审示例。
- 验证 Artemis 真实队列双向传输及 Rhea First／Second，详见 docs/PROTOCOL_PROFILE.md。

- 不可变应用属性编辑。
- 端点与链路诊断快照。
- 增加回归测试、独立可运行示例与 CI 示例步骤；详见 docs/EXPANSION_20261106.md。

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
