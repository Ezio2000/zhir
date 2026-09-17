# 研发接入 API

0.3.0 统一使用会话、operation 和资源契约。完整可运行示例位于
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
调用 `seal_user_input` 后才允许正常完成。运行中的控制接口包括：

| 方法 | 含义 |
| --- | --- |
| `input(message, source)` | 持久化用户/外部消息，按会话能力发送或留给下一轮 |
| `pause(suspension)` | 保存暂停状态，停止当前驱动任务 |
| `reply_operation(id, value)` | 向已绑定 operation 发送输入；先记录结果不确定边界 |
| `cancel_operation(id)` | 取消指定 operation；最终状态由完成事件确认 |
| `update_profile(profile)` | 按能力修订会话 profile；忙碌会话需声明 ProfileUpdates |
| `interrupt_output()` | 原生输出打断；output_epoch 与命令在同一提交中更新，输入不失效 |
| `flush_input()` | 需 FlushInput 能力；请求合成已缓冲输入，保持会话和输入端口开放 |
| `set_input_audio_enabled(enabled)` | 需 InputAudioControl 能力；持久化并设置远端音频输入处理模式，媒体端口保持开放 |
| `seal_user_input()` | 关闭输入并排空已接收媒体，再通知模型 |
| `cancel()` | 绕过控制队列发出整个运行的取消信号 |

除 cancel 外，ControlReceipt 中的 revision 表示内核已提交控制意图，不代表外部服务
已执行命令。随后通过 checkpoint、operation 和服务端确认观察实际状态。

## 模型会话与端点协议

实现 core 的 Model：`capabilities()`、`negotiate(&ModelRequest)`、
`open_session(SessionOpen)`。SessionOpen 包含稳定 session ID、本地已提交事件序号 after_sequence、output_epoch、
限制、初始上下文与配置、上下文/输入/profile 版本、运行模式、RecoveryRef 和运行上下文。
Ready 表示建立完成。声明 ExplicitGeneration 的模型由 Generate 发起推理；Live 打开后
直接接收原生事件，不伪造生成请求或生成结束。

ModelSession 的输入/输出端口分别实现 SessionSender/SessionReceiver。输入端口必须
报告绑定模型的能力和协商结果。会话事件使用单调 sequence；完整 Output 具有独立
item、caller 和可选 generation 标识。Operation 事件指向原始 item 的 CallRef。提供可恢复服务的适配器应尽早
发送 Recovery 或带 recovery 的 Acknowledged，并在重连时确认已经接收的命令。

普通请求型服务可使用 FunctionModel：回调接收 ModelRequest/ModelContext，返回
GenerationOutput。`context.deltas` 可增量发送观察数据，最终输出仍由 GenerationOutput 给出。
FunctionModel 和 HTTP 适配器不支持原生双向、运行中 steering、异步结果或服务端恢复；
不能通过修改 CapabilitySet 把这些协议声明成支持。

HTTP feature 为 `openai-chat`、`openai-responses`、`anthropic`。ModelConfig 接收
base URL、CredentialProvider 和模型 ID；客户端与超时可由调用方配置。默认
CapabilitySet 只描述协议默认能力，实际端点能力应显式提供。核心包不硬编码模型列表。

`ProtocolExtension` 由工厂为每次交换创建，负责请求扩展、输出解析与回放；
`ExtensionChain` 组合多个扩展。服务端工具通过 ProviderToolAdapter 绑定具体协议项，
声明识别、状态、输出与原生回放位置。未知的原生工具项必须有明确归属。

## MiniMax 双向 TTS

直接依赖 `zhir-minimax`，使用 `zhir_minimax::tts::TtsConfig`；不需要启用 HTTP 协议 feature。

```rust
use std::sync::Arc;
use zhir_models::credentials::StaticCredential;
use zhir_minimax::tts::{self, TtsConfig};

let credentials = Arc::new(StaticCredential::new("Bearer", api_key));
let mut config = TtsConfig::new("speech-2.8-hd", "male-qn-qingse", credentials);
config.voice.speed = 1.0;
let model = tts::model(config)?;
```

api_key 可使用普通按量 API Key 或 Token Plan Key：二者走相同的 Bearer 鉴权，
SDK 不检查 Key 前缀、不选择计费模式，也不会在额度耗尽时切换另一把 Key。
权限与计费由 MiniMax 服务端判断。当前线上验收使用 Token Plan Key；普通 API Key
覆盖了本地握手测试，尚未验证真实按量计费。测试目录的 cc-switch 启动脚本只读取
Token Plan Key，这个限制不属于 SDK。

返回的 WebSocketModel 直接交给 Runtime::builder。需要配置 ResourceStore 来封存音频。
使用 Interactive 模式，同时消费 media_output 和驱动 result；通过 control.input 追加
用户文字，通过 flush_input 合成已缓冲文字并继续输入，通过 seal_user_input 收完尾音并结束，
通过 interrupt_output 打断后继续输入。空格和换行片段按原文保留。
[完整示例](../crates/zhir-minimax/examples/minimax_tts.rs) 演示这些端口的组合。

Generate 只合成请求中最后一条 User 文本，不把整个历史读出来。AudioSettings.format
可选择 MP3、PCM、FLAC、WAV、原始/WAV μ-law，默认 MP3。μ-law 需要 8 kHz。
文档中的 Ogg/Opus 在当前 Token Plan 实测返回缺少音频或截尾的容器，因此没有加入生产配置。

每句使用独立 stream_id，sequence 从零开始，end 结束该句。消费者按 stream_id/epoch
分别处理，独立解码完整容器；被中断的句子可以不完整。
PCM 的 media_type 明确 s16le、采样率和声道。
流式 WAV 的 RIFF/data 长度未知，宿主导出可寻址文件时须在 end 后补齐长度，SDK 保留原始流。

connection 配置完整 WebSocket URL、建连/写超时、command_timeout、心跳间隔和消息大小上限。
启动、flush、取消和结束确认各有独立于心跳活动的等待期限。language_boost 与
pronunciation_dictionary、情绪、英文归一化、公式朗读、音色混合、语音效果、字幕粒度和
continuous_sound 都是 TtsConfig 的供应商参数。效果处理仅适用于流式 MP3；公式朗读
需要显式 Chinese；音色混合使用空 voice_id 和 1–4 个 timbre_weights。
服务端返回的格式、采样率或声道与请求不一致时失败，不错误标注音频。
字幕原始字段作为 ProtocolEvent 交付，不凭空构造时间戳。
CredentialProvider 的 audience 是连接 URL；`header:` metadata 用于额外账号头，
不能覆盖握手控制头。建连 401 只刷新一次，凭据解析也受建连超时与运行截止时间约束。
连接中断不会重发文本。服务端队列拒绝且无法关联具体输入时，报告 Uncertain。

该模型支持文本输入、音频输出、Steering 和 Streaming，不提供麦克风输入、工具、
运行中 profile 更新或断线恢复。声音参数通过 TtsConfig 设置；
不支持的 generation/extension 参数会被拒绝。Input 确认只表示适配器已发送文字，
flush、取消和任务完成分别等待远端 task_flushed / task_canceled / task_finished。
协议观察通过原生 SessionReceiver 的 Delta 输出，完成元数据保留最新 extra_info。

## 同步与异步工具

RuntimeTool 是唯一可执行工具接口，`start(call, context)` 和
`recover(record, context)` 都返回 `ToolExecution`：

- `Finished(OperationOutcome)`：只包含 Success、Failure 或 Cancelled。
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
使用 v4。持久化运行可生成 SuspensionTicket，通过
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
TransformModel 的 prepare 处理打开上下文和 ReplaceContext；命令与事件可以单独变换。
Generate 仅引用已确认的上下文/profile 版本和输入位置，不再携带全量历史。
Append 用 Submitted/Accepted 区分宿主输入与内核确认的输出投影，后者不能回显给原生服务。
ResponseFinished 的 input_position 只覆盖真实处理过的输入；结束 A 不能吞掉后来提交的 B。
一个会话最多一项活动生成，工具、输入和媒体仍可并发。异步变换需要遵守取消和预算。

## 资源与媒体流

Content::resource 接收 ResourceRef，或用 ResourceInput 同时指定 ResourceUsage。
来源为 Inline bytes、URL、Stored key 或 Provider reference；来源与使用精度分离。

ResourceStore::create 返回分块 writer，append 接收顺序号，finish 封存不可变引用。
ResourceStore::open 返回 reader，每次 read 指定最大字节数。MemoryResourceStore 总是
可用；FilesystemResourceStore 需要 `resources-filesystem`。同 key 相同数据幂等，
不同数据明确冲突。

`ResourceModel::new(model, store, max_input_bytes)` 在打开上下文、ReplaceContext 和
Append（包括工具结果及确认输出）中解析资源。一个请求/命令内按实际物化字节消耗总预算；原生回放中同一
数据有多份表示时分别计入。超限应改用媒体流。输出资源先封存，再进入 kernel 历史；
原生 JSON 中的媒体位置需显式绑定，防止内联副本继续进入回放数据。

原生双向模型提供可选 MediaSender/MediaReceiver。宿主通过 Invocation 的 media_input
和 media_output 使用独立媒体通道，同时驱动 result/control。每个 MediaChunk 显式带
session_id、stream_id、epoch、sequence、timestamp_us、media_type、bytes、end。
媒体序号在同一流内递增。输入 epoch 固定为 0；输出打断后使用新的 output_epoch，过期输出不会交付给宿主。结束的流从活动表移出，通过 media_archive 保留资源引用。

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

### GPT-Live 原生会话

直接依赖 `zhir-openai`，使用 `zhir_openai::live::LiveConfig` 注入宿主的
`CredentialProvider`，再调用 `zhir_openai::live::model(config)`，返回共享的
`zhir_models::WebRtcModel`。凭据元数据需要
`header:ChatGPT-Account-Id`；登录、OAuth 刷新和代理配置归宿主。
`zhir-openai/examples/gpt_live.rs` 演示 Runtime、资源存储、输入 Opus 包、输出消费和 SealUserInput。

完整发言通过 `ConversationItem` 写入历史；服务端识别的用户发言不会再发回模型。
`RuntimeBuilder::delegation(Arc<dyn DelegationHandler>)` 注入后台任务执行器。
执行器返回 `OperationHandle`，可以内部调用普通 Runtime；`OperationUpdate::Context`
发送可持久化进度，`Finished { outcome: OperationOutcome }` 提交最终结果。
这条路径复用操作身份、并发预算、取消、恢复与结果提交，不需要注册同名函数工具。
模型提供方的委托、RuntimeTool、ProviderToolCall 是三个明确的语义。


Live 的 `set_input_audio_enabled(false/true)` 对应订阅端 `input_audio.pause/resume`，
适配器等待 `input_audio.paused/resumed` 后确认。它控制远端音频输入处理，保持本地
媒体端口开放，不推进输出代次。结束时保留远端会话 ID、启动元数据和原生 usage。
DataChannel 与 RTP 使用独立有界队列；音频背压不会阻断控制确认。
事件出口暂满时也继续处理远端确认；待投递事件达到上限时明确报告容量错误。
Live 输出的 `max_buffered_media_bytes` 按实际负载字节计费，同一份预算贯穿 RTP
接收、待投递输出和媒体端口，消费后释放；输入使用另一份预算。每个方向另有
4096 块上限，空结束标记只占块数。事件队列容量与最大单块大小不决定音频包数。
RTP 无法保证向远端施加背压，超出接收预算会以 Uncertain 终止。
直接使用 ModelSession 时，SealUserInput 同样关闭媒体输入准入并排空已接收包；
排水期间继续处理远端事件、取消和固定确认期限，后续远端关闭动作等待排水完成。
Close 在收到远端确认、排空接收数据后结束事件端口，无需再次发送 Close。

Live 在原 PeerConnection 短暂 Disconnected 后，按 `LiveConfig.reconnect_timeout`
等待连接恢复，默认 10 秒；期间暂停新的发送，已有命令的确认期限不延长，不重发已发送命令。
这条路径有真实 UDP 断流成功记录，也曾触发固定确认期限而进入 RecoveryRequired，
不能宣称网络故障下始终恢复成功。程序 InterruptOutput 和连接销毁/进程重启后的会话恢复
尚未实现；已实测的控制命令被拒绝，侧带重连没有恢复主媒体通道，fork 被账号访问控制拒绝。公开 Live 的 fork 派生新会话，
不能直接代替原连接和未确认命令的恢复。当前订阅入口拒绝公开 API 的 store 参数；
这不等于 GPT-Live 服务端没有存储或恢复能力。详见独立接入包的
[已实现能力与限制](../crates/zhir-openai/README.md)。

这两种适配器的职责与剩余边界如下；不能通过扩充核心枚举来补出远端没有确认过的行为。

| 范围 | 归属 | 当前实现与边界 |
| --- | --- | --- |
| 控制、恢复引用、能力协商 | core 定义契约，kernel 持久化命令与执行状态 | FlushInput、InputAudioControl、InterruptOutput、恢复处置和输出 epoch 共用同一执行路径 |
| 有界端口、背压、旧 epoch 过滤、终止交付、确认期限 | models 的 native 共享实现 | Live 与 MiniMax 共用；超时保留所等事件及命令，供应商负责匹配回执 |
| 合成格式与声音参数 | MiniMax 适配器 | 六种格式及当前 task_start 参数已接入；Ogg/Opus 实测截尾，未声明支持；动态改声没有对应的已文档化 bidi 命令，session_id 仅用于关联 |
| 双向 Opus/RTP、输入启停、转录、client delegation | Live 适配器 | 已接入；暂时断网只等待原 PeerConnection 恢复，不创建另一条会话 |
| 程序输出打断、销毁连接后恢复 | Live 适配器依赖的订阅协议 | 探测命令被拒绝；侧带可重连但未恢复主媒体通道和 outbox，fork 被访问控制拒绝，未声明支持 |
| 直接 RuntimeTools 与后台 profile 更新 | Live Responses delegation | OAuth 创建成功，但 response.create 在服务端解析后台地址失败；没有保留无法验证的生产实现分支 |
| 图片、结构化结果、推理模型配置 | 处理 client delegation 的后台模型 | 属于后台模型的能力，不转换成 Live 语音前端的同名能力 |
| 播放设备、解码、WAV 文件封口 | 宿主应用 | 媒体按 stream/epoch 消费；设备缓冲由宿主清理，文件导出不改写已提交媒体 |

供应商依据：[MiniMax bidi](https://platform.minimax.io/docs/api-reference/speech-t2a-websocket-bidi)、
[Live delegation](https://developers.openai.com/api/docs/guides/live-delegation)。
真实账号限制与成功记录见 ignored `test-results/native-support/`，不是所有账号的能力保证。
