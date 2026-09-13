# 研发接入 API

0.2.0 统一使用会话、operation 和资源契约。完整可运行示例位于
[custom_tool.rs](../crates/zhir/examples/custom_tool.rs)、
[resume.rs](../crates/zhir/examples/resume.rs) 和
[chat.rs](../crates/zhir/examples/chat.rs)。仓库验收用模型、记录器与消费者测试统一位于不发布的 `zhir-testing`；发布的 SDK 不依赖它。

## 创建运行

`Runtime::builder(model)` 接收 `Arc<dyn Model>`。按需注入 `runtime_tools`、`store`、
`resources`、`approval`、`scheduling` 和 `history_reducer`。`defaults` 配置默认
RunOptions；RunRequest 按整个字段覆盖默认值，创建后冻结。运行控制产生的 profile 修订
存入 SessionSnapshot，不改写初始 RunOptions。

```rust
let runtime = Runtime::builder(model)
    .runtime_tools(registry)
    .store(store)
    .resources(resources)
    .build()?;
let mut invocation = runtime.start(RunRequest::new([Message::user("开始")]))?;
let control = invocation.control();
let completion = invocation.result().await?;
```

`start` 创建惰性 Invocation；`Invocation::start`、`result`、`events` 或媒体端口选择
启动执行。`result` 返回 RunCompletion；Completed、Suspended、Failed、Cancelled、Limited
都属于已结算状态。存储错误和 CAS 冲突返回携带最后已知 checkpoint 的 RunError。
丢弃 Invocation 或未读完的 EventStream 会取消运行；单纯读取结果无需消费观察事件。

`RunMode::Task` 在当前轮结束且操作已完成时结算；`Interactive` 等待追加输入，
调用 `end_input` 后才允许正常完成。运行中的控制接口包括：

| 方法 | 含义 |
| --- | --- |
| `input(message, source)` | 持久化用户/外部消息，按会话能力发送或留给下一轮 |
| `pause(suspension)` | 保存暂停状态，停止当前驱动任务 |
| `reply_operation(id, value)` | 向已绑定 operation 发送输入；先记录结果不确定边界 |
| `cancel_operation(id)` | 取消指定 operation；最终状态由完成事件确认 |
| `update_profile(profile)` | 按能力修订会话 profile；忙碌会话需声明 ProfileUpdates |
| `interrupt()` | 原生中断；epoch 与命令在同一提交中更新 |
| `end_input()` | 关闭输入并排空已接收媒体，再通知模型 |
| `cancel()` | 绕过控制队列发出整个运行的取消信号 |

除 cancel 外，ControlReceipt 中的 revision 表示内核已提交控制意图，不代表外部服务
已执行命令。随后通过 checkpoint、operation 和服务端确认观察实际状态。

## 模型会话与端点协议

实现 core 的 Model：`capabilities()`、`negotiate(&ModelRequest)`、
`open_session(SessionOpen)`。SessionOpen 包含稳定 session ID、恢复游标、epoch、
限制、初始请求、RecoveryRef 和运行上下文。打开会话只建立通道；推理由 StartTurn 发起。

ModelSession 的输入/输出端口分别实现 SessionSender/SessionReceiver。输入端口必须
报告绑定模型的能力和协商结果。会话事件使用单调 sequence；完整 Output 具有 turn、
item、caller 标识。Operation 事件指向原始 CallRef。提供可恢复服务的适配器应尽早
发送 Recovery 或带 recovery 的 Acknowledged，并在重连时确认已经接收的命令。

普通请求型服务可使用 FunctionModel：回调接收 ModelRequest/ModelContext，返回
TurnOutput。`context.deltas` 可增量发送观察数据，最终输出仍由 TurnOutput 给出。
FunctionModel 和 HTTP 适配器不支持原生双向、运行中 steering、异步结果或服务端恢复；
不能通过修改 CapabilitySet 把这些协议声明成支持。

HTTP feature 为 `openai-chat`、`openai-responses`、`anthropic`。ModelConfig 接收
base URL、CredentialProvider 和模型 ID；客户端与超时可由调用方配置。默认
CapabilitySet 只描述协议默认能力，实际端点能力应显式提供。核心包不硬编码模型列表。

`ProtocolExtension` 由工厂为每次交换创建，负责请求扩展、输出解析与回放；
`ExtensionChain` 组合多个扩展。服务端工具通过 ProviderToolAdapter 绑定具体协议项，
声明识别、状态、输出与原生回放位置。未知的原生工具项必须有明确归属。

## 同步与异步工具

RuntimeTool 是唯一可执行工具接口，`start(call, context)` 和
`recover(record, context)` 都返回 `ToolExecution`：

- `Finished(RuntimeToolOutcome)`：只包含 Success、Failure 或 Cancelled。
- `Active(OperationHandle)`：包含 recovery reference、OperationControl 和 OperationEvents。

工具不得把“已受理”或“等待”包装成成功结果。异步工具以 Running/Waiting/Progress/
Finished/Unknown 更新生命周期；Waiting 的 prompt 用于展示交互，只有 Finished 才成为
下一轮模型输入。OperationControl 的 reply/cancel 调用也不是最终完成确认。

`RuntimeToolContext.operation_id` 是 kernel 在 start 前持久化的操作标识，可用于外部
幂等键。`recover` 必须查询或附着到原任务，不能把无法恢复的任务重新 start。
无法判断外部结果时报告 Unknown，由调用方明确解决。

RuntimeToolRegistry 校验目录、输入与最终输出，也校验 Active 事件里的最终输出。
Catalog 是不可变快照，bind 必须返回与声明一致的 specification。输入显式区分
`RuntimeToolInput::Structured` 和 `Freeform`。

立即完成的结构化工具可使用 TypedTool，输入 Deserialize 与输出 Serialize 类型共同
决定 Schema。ToolReply 只表示最终成功数据和可选展示内容：

```rust
let tool = TypedTool::<EchoArgs, String>::new(
    "echo", "Return supplied text", Execution::default(),
    |args, _context| async move { Ok(ToolReply::success(args.text)) },
)?;
```

`ToolReply::content` 可以指定资源或文本展示。无类型 JSON 结果使用
`runtime_tools::reply::json`；复杂异步生命周期直接实现 RuntimeTool。
重试装饰器只按显式执行事实重试，不能把 Active 工作视为失败后重新发起。

## 恢复与子 Agent

RunCompletion 可提取不可变 checkpoint。`wire::encode_checkpoint/decode_checkpoint`
使用 v2。持久化运行可生成 SuspensionTicket，通过
`ResumeRequest::from_ticket` 校验 run、revision、checkpoint 与 suspension。

用 `ResumeRequest::resolve` 为未完成操作提供明确处置：

| RecoveryResolution | 语义 |
| --- | --- |
| `Attach { operation_id, reference }` | 提供适配器恢复引用，附着到已有工作 |
| `Complete { operation_id, outcome }` | 提交外部已经核实的最终结果 |
| `Abandon { operation_id, reason }` | 明确放弃，产生取消结果 |

`resume` 只接受 Suspended；进程中断留下的 Running checkpoint 使用 `continue_from`。
两者在打开适配器和恢复工具前提交 Attached CAS。运行不会重发已标记 sent 的命令。
没有可用会话恢复引用的未确认请求会保持 RecoveryRequired，普通 HTTP 轮次服务无法
凭空恢复远端请求。提供具备恢复能力的会话适配器，或在产品层完成外部核对与处置。

内置 `ask_question` 产生 Waiting operation。
`builtins::interaction::response(checkpoint, operation_id, answers)` 校验答案并创建
Complete resolution。[resume 示例](../crates/zhir/examples/resume.rs) 展示完整闭环。

`agent_run` 也是普通异步 RuntimeTool。`agent` feature 允许注入任意 AgentBackend；
`agent-runtime` 提供 `RuntimeAgentBackend::new(runtime, system_prompt, max_running)`。
子运行标识来自父 run/operation，继承截止时间；后台仅驱动同一个 kernel。
父端脱离后保存子运行的暂停状态，恢复读取子 checkpoint；取消暂停中的子任务也会
提交子运行的 Cancelled 状态。

## 能力协商与实际确认

RequestProfile 包含 generation、serving、reasoning、language、interaction、显式备选
值 alternatives 和按命名空间组织的 extensions。`Requirement::Required` 不满足就失败；
Preferred 只允许使用调用方列出的替代值，未满足项记录原因。

例如，`serving = Required(LowLatency)` 只表达低延迟要求。HTTP 接入通过
`ProfileMapping::new(key, semantic_value, endpoint_field, wire_value)` 声明端点映射。
是否映射到服务等级、fast 参数或其他字段由接入方确定，不由模型名称猜测。
每个 ResourceInput 的 `usage.fidelity = Required(Original)` 表达原图精度要求。
端点必须声明支持，编码器才能使用相应图片 detail 值。

协商的 selected 值不等于实际执行确认。SessionSnapshot 同时保存 negotiated 与
`EffectiveProfile`，后者的每个字段是 Provider、Verified 或 Unknown。适配器只能用
服务端信息或已验证证据填入确认；不能复制请求值假装服务已执行。原始图像、服务
等级与推理选项仍需要具体账号/模型/端点支持。

RetryingModel 仅重试尚未发送命令的会话建立。FallbackModel 接收
`FallbackCandidate::new(stable_id, model)` 列表，各候选独立协商；恢复绑定原 ID，
顺序变化不会把任务迁移到另一服务。ConcurrencyLimitedModel 的共享许可覆盖会话寿命。
TransformModel 可变换打开请求、StartTurn、命令与事件；异步变换需要遵守取消和预算。

## 资源与媒体流

Content::resource 接收 ResourceRef，或用 ResourceInput 同时指定 ResourceUsage。
来源为 Inline bytes、URL、Stored key 或 Provider reference；来源与使用精度分离。

ResourceStore::create 返回分块 writer，append 接收顺序号，finish 封存不可变引用。
ResourceStore::open 返回 reader，每次 read 指定最大字节数。MemoryResourceStore 总是
可用；FilesystemResourceStore 需要 `resources-filesystem`。同 key 相同数据幂等，
不同数据明确冲突。

`ResourceModel::new(model, store, max_input_bytes)` 在打开请求、StartTurn、实时 Input
和 ToolResult 中解析资源。一个请求/命令内按实际物化字节消耗总预算；原生回放中同一
数据有多份表示时分别计入。超限应改用媒体流。输出资源先封存，再进入 kernel 历史；
原生 JSON 中的媒体位置需显式绑定，防止内联副本继续进入回放数据。

原生双向模型提供可选 MediaSender/MediaReceiver。宿主通过 Invocation 的 media_input
和 media_output 使用独立媒体通道，同时驱动 result/control。每个 MediaChunk 显式带
stream_id、turn_id、epoch、sequence、timestamp_us、media_type、bytes、end。
媒体序号在同一流内递增；中断后用新 epoch。过期 epoch 不向模型/宿主交付。

媒体在存储完成和 cursor 提交后才交付，输出消费者变慢会向上游施加背压。
checkpoint 只记录最新封存节点；SealedMedia 的 previous 引用链接历史节点。
长期资源保留、检索索引、解码/转码与播放界面由产品层负责。

## 凭据与跨供应商能力

ModelConfig.credentials 接收 CredentialProvider。StaticCredential 适合固定 token；
RefreshingCredential 接收刷新回调，按 audience 缓存，按过期时间刷新，并对被服务端
拒绝的 generation 做失效处理。Credential.metadata 的 `header:` 项用于适配器需要的
额外账号请求头。401 只触发一次刷新重试；发送后的其他失败不自动重放推理请求。

这能承载 OAuth access token、账号标识与刷新结果，但不提供 Codex 登录、浏览器回调、
ChatGPT 聊天记录访问或订阅权益管理。OAuth 登录和端点规则由宿主接入层负责。

OpenAI 模型调用另一供应商的视频/语音服务时，把该服务实现为 RuntimeTool operation；
使用某供应商模型自带的视频/语音能力时，由 ModelSession/ProviderToolAdapter 报告
provider operation 或媒体流。供应商任务 ID、轮询/推送、取消和回放映射属于该适配器。
这两条路径共用资源与恢复契约，不需要为具体供应商向核心增加分支。

## 历史、输出与观察

HistoryEntry 保存稳定 ID、CallRef 与 Message。History::messages 是到达顺序视图；
`model::conversation(history.entries())` 才是新一轮模型的逻辑会话投影。后者合并同轮
assistant 输出与完成信息，把异步 provider 更新归回原始调用。用持久化 checkpoint
分析因果，不要用观察事件重建事实。

JsonOutput<T>（`typed-output`）从同一 Schema 构造请求格式并验证最终结果。
`runs` 中的便利函数建立在普通 Invocation 上。观察流可能丢弃事件并发出 ObservationGap；
审计、回放与统计应使用 checkpoint/RunStore 或测试模块中的 RecordingStore。
