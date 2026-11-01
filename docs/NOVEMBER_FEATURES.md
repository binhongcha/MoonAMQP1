# MoonAMQP1 十一月功能扩展

应用、网关和工具需要交换带类型的消息，并正确处理分片、链路额度、会话窗口和确认状态。已有 AMQP 0-9-1 客户端不能替代线协议与状态模型不同的 AMQP 1.0。项目让 MoonBit 宿主复用同一协议逻辑，并在提交发送前确定资源是否足够，避免任务在本地已经过期仍进入发送流程。

## 1. 完整交付发送预检

`Endpoint::plan_send`：对消息、tag、协商帧长、link credit、session window、未确认上限和输出队列执行与 send 同源的检查，返回准确帧数和 wire 字节数。重复预检不消耗额度、ID或队列；计划不预留资源，状态改变后 send 会重新检查。

复杂度与代价：编码和分片成本约 O(M·D+F)，临时空间 O(M+F)；预检后发送会再次编码，不能当作零成本估计或发送预约。

## 2. 显式消息到期判断

`Message::expiry_status`：读取 header TTL 和 properties.absolute-expiry-time，以宿主提供的 UTC 毫秒判断较早到期条件，返回剩余毫秒和过期标志。TTL 从本地 acquisition 时刻算起；拒绝时钟倒退，不自行删除、重投或改写消息。

复杂度与代价：按 section 数扫描；时间差使用无符号表示避免跨 Int64 极值溢出，不通过不安全的时间戳相加求截止。

## 运行

```sh
moon run examples/november --target native --deny-warn
moon check --target all --deny-warn
moon build --target all --deny-warn
moon test --target all --deny-warn
```

Windows当前工具链路径问题沿用README的项目适配脚本；正式命名空间为 `binhongcha/moonamqp1`，安装方式见README。

## 范围

不实现 broker、AMQP 0-9-1、持久 outbox、SASL/TLS、自动重连、事务或业务 exactly-once；TTL的跨宿主时钟可信性、前一跳停留时间及队列策略由宿主处理。

Rhea 双向类型/消息/帧与 5KB 活动对端验证；新增预检和到期边界测试。 具体当前执行结果由本地检查记录提供，不以CI配置代替远端成功记录。
