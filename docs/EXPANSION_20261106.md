# MoonAMQP1 增量功能：2026-11-06

状态：本地未发布增量。使用本目录源码运行，已发布的 0.1.0 不包含本页新增 API。

## 不可变应用属性编辑与端点与链路诊断快照
`Message::with_application_property(name, value, limits~)` 在完整消息校验下编辑 application-properties。
`Some(Null)` 是显式 AMQP null，`None` 删除属性；保留其他属性的相对顺序，将编辑项放在该节末尾。
缺省节按规范位置插入，删除最后一项时移除空节。载荷、其他节及原消息均保留。
属性名称、标量类型、值深度、总消息大小仍受原 Limits 限制；不接受复合应用属性值。

`Endpoint::diagnostics()` 返回队列字节、部分输入字节、待消费事件数和会话状态，
每个链路包含句柄、角色、地址、credit、收发未结算数和接收分片状态。
结果数组可由调用方修改，不消耗事件、不 drain、不调整 credit；它反映当前本地状态而非远端实时指标。

用于任务重试标记、事件路由元数据、嵌入式消息客户端的背压监控和连接故障定位。
不增加 broker、自动重试或网络连接管理；重投策略仍由应用决定。

属性编辑时间/空间 O(S+V+B)，S 为节数、V 为编码校验工作、B 为消息大小；
诊断时间 O(L+U)，空间 O(L)，L 为链路数、U 为收发未结算交付数。
测试包含 null/删除区别、UInt64 最大值、载荷不变、预算拒绝，以及真实双端握手/发送/确认后的快照隔离。

## 运行

```sh
moon run examples/expansion --target native --deny-warn
moon test --target all --deny-warn
```

Windows 使用 README 中对应的 Moon 包装脚本替代 `moon`。
CI 已添加该示例；无需网络服务、私钥或真实设备。
