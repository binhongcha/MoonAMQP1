# MoonAMQP1

MoonBit 原生 **AMQP 1.0 类型、消息、增量分帧与有界协议状态核心**。同一实现用于 native、JavaScript、Wasm、Wasm-GC。核心不绑定 Rhea、C 消息库或网络运行时；原生示例宿主使用 async 处理文件和标准输入输出。

模块：`binhongcha/moonamqp1@0.1.0`；[GitHub](https://github.com/binhongcha/MoonAMQP1) · [Mooncakes](https://mooncakes.io/docs/binhongcha/moonamqp1)。

## 生态定位与使用场景

AMQP 1.0 的类型、described value、会话窗口、单向 link、credit 与 settlement 构成跨系统消息交换能力。[RabbitMQ 官方](https://www.rabbitmq.com/docs/specification)明确它与 AMQP 0-9-1 使用不同线协议和客户端。已有 MoonBit 0-9-1 库仍适用于其自身协议；本项目补充独立的 1.0 协议核心，不提供 0-9-1 exchange/queue 方法或透明转码。

- **后台任务与服务作业**：发送 5KB 作业，只有一个 credit，消息分为多个 transfer；工作端明确 accepted 或 released，上层自行决定是否重试。适用于文件处理、构建任务、通知作业、订单异步步骤。运行 `examples/tasks`。
- **设备和应用遥测**：慢速历史链路没有 credit 时，实时样本和告警链路仍能使用共享 session；精确传递 uint64 ID、timestamp 和单位。适用于采集网关、监控、应用事件和边缘上报。运行 `examples/telemetry`。
- **跨系统消息桥与 Web/Wasm 宿主**：两个独立连接保留带类型的属性、注解、多个 data section 与 footer，下游接受后再确认上游。适用于消息代理适配、迁移工具、离线诊断和可嵌入集成测试。运行 `examples/bridge`；该场景可直接编译为 JS，不附带浏览器 UI。

本地已有项目分别处理文件格式、HTTP 缓存、更新信任、编码恢复、事件日志挖掘等，本项目处理的是 AMQP 1.0 线上消息与链路状态。检索属于有日期、有关键词范围的排重证据，不能证明整个生态绝无相近工作。

## 安装与运行

安装[官方 MoonBit 工具链](https://docs.moonbitlang.com/en/latest/download-and-install.html)。本机验证 `moonc 0.10.16`，符合赛事最低要求；CI 固定 compiler/core 0.10.16+b1a0e846e；依赖版本见 `moon.mod`。

```sh
moon version --all
moon check --target all --deny-warn
moon build --target all --deny-warn
moon test --target all --deny-warn
moon run examples/tasks --target native --deny-warn
moon run examples/telemetry --target native --deny-warn
moon run examples/bridge --target js --deny-warn
moon run examples/scale --target native --deny-warn
moon fmt --check
moon info
git diff --exit-code
```

从项目目录运行即可复现，示例已配置导入。Moon20261016 的 Windows OCaml 子工具可能拒绝 `\\?\` 路径，可使用进程参数适配器；首次构建需要 MSVC Build Tools，不修改全局环境或编译器：

```powershell
.\tools\windows\moon.ps1 check --target all --deny-warn
.\tools\windows\moon.ps1 test --target all --deny-warn
.\tools\windows\moon.ps1 run examples/tasks --target native --deny-warn
```

## 最小使用样例

在已有 MoonBit 工程中安装：

```sh
moon add binhongcha/moonamqp1@0.1.0
```

使用方 `moon.pkg` 配置导入：

```moonbit
import { "binhongcha/moonamqp1" @amqp, }
pkgtype(kind: "executable")
```

```moonbit
fn main raise {
  let message = @amqp.Message::data(b"payload", properties=[
    ("sequence", ULong(18446744073709551615UL)),
    ("kind", Text("job")),
  ])
  let bytes = @amqp.encode_message(message)
  let restored = @amqp.decode_message(bytes)
  assert_eq(restored.data_bytes(), b"payload")
}
```

消息编解码可以独立使用。`Message::new` 接受已知 section 的有序列表；`sections()` 返回深层副本，调用方修改数组不会改变消息。无 body、`amqp-value(null)` 和空 data section 保持区别。`data_bytes()` 连接 data sections，遇到 sequence/value body 会报错。

## 兼容范围

| 层次 | 支持 | 当前边界 |
| --- | --- | --- |
| 值 | 全部原始域、十进制原始字节、精确整数/时间戳、list/map、typed array、数字或符号 descriptor | 十进制不提供算术；数组公共 constructor 最多一层 descriptor；编码采用全宽 canonical 形式 |
| 文本 | 严格 UTF-8、Unicode scalar、ASCII symbol，保留 BOM 和 ASCII NUL | 拒绝非法 UTF-8 与未配对 UTF-16 代理项，不静默替换 |
| 帧 | AMQP1.0 header、9 类 transport performative、扩展头、空 heartbeat、任意 feed 分块 | SASL frame/type、TLS header、未知额外 performative 字段明确拒绝 |
| 消息 | header、注解、properties、application-properties、data/sequence/value body、footer | 检查顺序、唯一性、互斥及字段域；未知 section 不静默跳过 |
| 状态 | 一个 connection、一个 session、多 link、本地/远端 channel 与 handle 独立映射 | 不支持 crossed begin、多 session、session/link 恢复、动态/持久 terminus、过滤及能力协商 |
| 收发 | link credit、session window、自动发送分片、接收组装/aborted、三种 sender settle mode、receiver first | 不支持 receiver second、drain、resume、事务、非零 message-format、分片中改变 settlement |
| 确认 | accepted、rejected（保留无 error 的拒绝）、released、modified 的两个布尔标志、范围 disposition | received/事务状态、modified 附加注解明确不支持；确认不等同业务 exactly-once |
| 宿主 | bytes/commands → bytes/events；原生 CLI 文件及 stdio 示例 | 无内置 broker、TCP 客户端、SASL、TLS、持久 outbox、自动重连、定时器或自动重试 |

首版终结点仅接受 idle-time-out 缺省或 0；定时器应由未来宿主集成实现。source/target 的非空高级字段会被明确拒绝，而不是假装完成过滤或 durability。typed value/message 层可独立处理状态核心尚不协商的内容。

`Value::ArrayValue(code, descriptor, bodies)` 的 code 使用全宽基础类型，如 uint `0x70`、symbol `0xb3`、list `0xd0`；bodies 不重复包含公共 descriptor。读取接受 array8/32 及 compact/zero-width constructor，再规范化。通用 map 保留顺序，键按类型与规范化值检查重复；注解、fields、application-properties 另有各自键域限制。

## 驱动连接与处理流控

1. 创建 `Endpoint`；调用 `start()`，将 `drain()` 字节交给宿主的可靠有序传输。
2. 每次输入调用 `receive(bytes)`，处理 `events()`，再发送 `drain()`。被动端收到协议 header 会自动响应 open。
3. 主动一方在 RemoteOpen 后调用 `begin()`，另一方自动接受单 session；收到 SessionReady 后 attach sender/receiver。
4. 接收 link ready 后调用 `grant(handle, credit)`。这设置以当前 delivery-count 为基准的新额度，不是累加旧额度。
5. sender 使用本地 handle 调用 `send(message, tag)`；收到完整 Received 后按业务结果 `acknowledge(id, outcome)`。tag 最多 32 字节，同一 link 的在途未确认标签必须唯一；标签不提供跨重连幂等性。
6. 显式 `refresh_windows(incoming, outgoing)` 重新通告宿主可承担的会话窗口；对端 flow 会更新远端容量。它与 grant link credit 是两个独立操作。
7. 正常结束用 `end_session()` 或 `close()` 完成相应握手；传输 EOF 调用 `finish_input()`，无正常 close 会被报告。

每个 transfer **帧**消耗一次会话窗口，一条多帧 delivery 只消耗一次 link credit。`send` 在编码全部分片并通过队列/窗口预检后才更新状态；失败可以在补足窗口或清空队列后重试。首版不暂存“发送一半等待窗口”的命令，因此整条 delivery 的帧数必须能装入当前两个发送窗口。其 frame count 可从前后 `session_positions()` 差值观察。

收到非法 wire 数据后，endpoint 永久失败，释放在途状态并清空待发送回复；不得拿同一实例继续解析。正常 detach 会产生 UnsettledLost，向宿主说明未确认结果不能再等待。连接/session 关闭使其所有未确认结果未知，宿主必须自行保留业务任务状态。核心对象不支持多个执行者并发修改，需串行驱动。

## 资源与复杂度

`Limits` 默认限制：单值/输入 feed/逻辑聚合接收载荷/输出队列 16MiB、单帧 1MiB、单消息 16MiB、总值节点 65536、嵌套32层、link256、事件/各方向未确认256、每消息4096片。可调整，深度上限128；本地 session window 和 link credit 限于不产生 serial 歧义的半范围。接受合法 UINT_MAX 远端 session 容量；不会据其大小直接分配内存。

增量分帧按字节追加，输入总长 N 时 O(N)，只保留一个未完成帧；消息分片按字节组装 O(M)。嵌套容器编码临时缓冲与 map key 规范化的成本至多 O(N·D)，D 为受限深度。handle 查找 O(L)，范围确认只扫描 O(P) 个已知 pending ID，排序最坏 O(P log P)，不会按对端声明的巨大 ID 区间逐个遍历。

预算限制的是逻辑字节和节点数量，不是精确进程 RSS：编码/解码副本、JS 对象、Message 树与宿主已取走的事件仍有开销。核心保留输出字节；`drain()` 不代表网络已可靠发送。`examples/scale` 验证2048条、6144帧、64条有界批次的持续正确性，不是跨语言速度基准。

## 独立互操作与 CLI

```sh
moon build --target native --deny-warn
npm ci --ignore-scripts --no-audit --no-fund
npm run interop
moon run cmd/main --target native -- value INPUT.bin NEW_OUTPUT.bin
moon run cmd/main --target native -- message INPUT.bin NEW_OUTPUT.bin
moon run cmd/main --target native -- frames INPUT.bin NEW_OUTPUT.bin
```

CLI 拒绝覆盖已有输出。`peer` 使用二进制 stdin/stdout 承载 AMQP1.0：为接收 link 授予32个 credit 并 accepted；为获额度的发送 link 发送一次5KB原创测试消息。它是互操作示例宿主，不是生产服务。

固定 Rhea3.0.5 为开发依赖：27组合法类型（含准确 UINT64、数组/描述类型）、消息 section 和帧流双向对照；真实 Rhea 客户端通过 stdio 与 MoonBit 双向收发5KB消息，协商512字节帧并验证分片、accepted、关闭。没有连接 RabbitMQ、Azure 或外部 broker，不能据此宣称这些服务已验证。Rhea 的宽松解码不作为恶意输入合法性的依据，截断/错类型/错状态/预算路径另行测试。

[Linux/Windows CI](https://github.com/binhongcha/MoonAMQP1/actions) 覆盖检查、构建、四后端测试、格式、接口、五个场景、独立 Rhea 互操作和包内容；工程验证不等于正式赛事验收。申报书及审查/AI/汇总材料可在项目本地文件夹保存，由忽略及历史/包检查排除，不进入 Git 或发行包。许可证为 Apache-2.0，来源与 AI 辅助范围见 [THIRD_PARTY.md](THIRD_PARTY.md)。规范参考：[Types](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html)、[Transport](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-transport-v1.0-os.html)、[Messaging](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html)。


## 十一月新增功能

**完整交付发送预检**（`Endpoint::plan_send`）；**显式消息到期判断**（`Message::expiry_status`）。详见[功能、用途和边界](docs/NOVEMBER_FEATURES.md)，运行 `moon run examples/november --target native --deny-warn`。功能包含于0.1.0，安装方式见上文。


## 本地开发增量（未发布）

新增 **不可变应用属性编辑**、**端点与链路诊断快照**，用途、限制和复杂度见 [功能文档](docs/EXPANSION_20261106.md)。
从本仓库源码运行 `moon run examples/expansion --target native --deny-warn`。
本节 API 尚未包含在已发布的 0.1.0 中。
