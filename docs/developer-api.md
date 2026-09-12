# 研发接入 API

以下 API 已实现。无需启用任何 HTTP 协议，即可运行强类型工具和自定义模型示例：

```sh
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --example resume --features interaction,memory
```

[custom_tool.rs](../crates/zhir/examples/custom_tool.rs) 展示函数式模型与强类型工具；
[resume.rs](../crates/zhir/examples/resume.rs) 展示保存、wire 往返和按暂停票据恢复。
供应商策略、业务工具及资源读取逻辑属于调用方。

RuntimeTool 是唯一可执行工具接口；ProviderToolSpec 声明服务端能力，ProviderToolCall
记录服务端执行。公开工具组件通过 `zhir::runtime_tools` 导出。

## 工具：类型驱动 Schema

```rust
#[derive(serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct EchoArgs {
    text: String,
}

let tool = TypedTool::<EchoArgs, String>::new(
    "echo",
    "Return supplied text",
    Execution::default(),
    |args, _context| async move { Ok(ToolReply::success(args.text)) },
)?;
registry.register(Arc::new(tool))?;
```

输入类型的 Deserialize 规则决定输入 Schema，输出类型的 Serialize 规则决定输出
Schema。生成标准 Draft 2020-12，保留嵌套定义和引用；注册表仍执行输入/输出校验。
改变字段不需要再改一份手写 JSON。Rust 的默认值由 Serde 提供，绑定过程不修改请求 JSON。

结构化参数必须是 JSON 对象。回调返回 `Result<ToolReply<O>>`；O 决定结构化结果的
Schema，ToolReply 决定成功、异步受理或等待状态，以及可选的媒体展示内容：

```rust
Ok(ToolReply::waiting("confirmation-1", receipt, "checkout")
    .content([Content::Image { source: image_source }]))
// 普通结果：ToolReply::success(receipt)
// 异步受理：ToolReply::accepted("job-1", receipt)
```

三种状态的结构化输出统一经过注册表校验。Waiting 同时生成模型可见结果和 Suspension；
自定义暂停元数据可用 `ToolReply::suspended(payload, suspension)`。Accepted 不自动暂停。
回调错误继续走 Result，转换为运行结果的错误行为不变。显式外部 Schema 和自由文本参数
仍用 FunctionTool / structured / freeform。执行事实由调用方声明。

通过 zhir 使用时启用 `typed-tools`，直接使用 zhir-tools 时启用 `typed`。调用方需要
serde 的 derive 和 schemars 来定义自己的类型。

## 模型与流回调

```rust
let model = FunctionModel::new(capabilities, |request, context| async move {
    context.cancellation.check()?;
    call_my_service(request, context).await
});

let sink = FunctionDeltaSink::new(|delta| async move {
    write_my_event(delta).await
});
```

两个业务函数由调用方提供。FunctionModel 校验请求与响应并在回调前后检查取消状态；
FunctionDeltaSink 等待写入完成，不新增缓冲或后台任务。共享客户端通常用 Arc 捕获，
在进入 async move 前 clone；闭包是可并发调用的 Fn。

通用组件由独立 `models` feature 导出，正常依赖图没有 reqwest。HTTP 协议需要显式
启用 openai-chat、openai-responses 或 anthropic。这些名称表示协议实现，不决定供应商。

## 扩展组合

```rust
let model = model.with_extension(|ctx| {
    let mut adapters = ProviderTools::new();
    // 用户函数可使用 ctx.protocol、ctx.request、ctx.run.metadata，失败直接返回。
    adapters.register(make_my_adapter(ctx.protocol, ctx.run)?)?;
    Ok(ExtensionChain::new()
        .push(adapters)
        .push(EventMapping::default()))
});
```

工厂接收只读 ExtensionContext，返回 Result；失败不会发起 HTTP 请求。工厂是同步的，
需要异步准备时使用 TransformModel。链在每次模型调用及重试尝试中重新创建，避免共享可变会话状态。所有 hook 按声明顺序运行。
请求和事件 hook 报错时停止；新增 delta 按顺序合并，整个事件 hook 成功后才返回。

响应 hook 逐级传递 Result，包括 Err。这样前面一个只修改请求的扩展不会阻断后面
针对新响应形状的解码器。后续扩展可以显式恢复前一阶段的错误；最终仍由 HttpModel
校验返回响应。链不猜测 JSON 合并规则，也不自动重排扩展。

## 协议参数与完整流观察

ModelConfig 接收调用方提供的 endpoint、凭据、模型标识、超时和 HTTP client。
协议声明的 Capabilities 是默认值；用 with_capabilities 描述实际模型支持的能力。

`ModelOptions.extra` 向编码后的请求递归添加对象字段，不替换已有叶值或数组；
input/messages/system/tools/tool_choice 等会话与工具字段保留给标准编码流程。
冲突返回字段路径，显式修改受控字段使用 encode_request hook。

原始 SSE 帧通过 ProtocolEvent 暴露 event/id/retry/data；data 可解析时为 JSON，
否则为字符串。包括已知帧及结束标记。框架不保存完整原始事件日志；未知增量语义由用户
会话累积到最终 ModelResponse。重试与 fallback 在任何 delta 已发出后停止重试。

需要完整接收模型 delta 时使用 `ObservedModel::new(model, factory)`。工厂按调用创建
用户 DeltaSink；每次 emit 等待写入，不增加队列或后台任务。下游 sink 先收到事件，观察器
随后写入，失败中止调用。放在重试器内侧时按尝试创建，外侧时按逻辑调用创建。观察器可能
保留失败或取消调用的部分事件；需随 checkpoint 持久化的最终语义应放入 ModelResponse。

RuntimeTool 的媒体结果按协议内容数组编码。纯文本 Chat/Responses 结果为字符串；
Messages 将连续工具结果组成同一 user 消息，保留顺序、调用 ID 和错误标记。
实际 endpoint 是否接受某个消息角色下的媒体，由消费者配置和实测确认。

## 异步请求准备

```rust
let model = TransformModel::new(inner, |mut request, context| async move {
    let text = load_my_document(&context.run).await?;
    request.messages.insert(0, Message::system(text));
    Ok(request)
});
```

变换函数拿到上下文副本，只返回 ModelRequest；运行身份、截止时间、取消信号和观察器
沿用原调用。函数在内层模型执行前完成，失败不会调用内层模型。

输入能力默认与内层模型相同。若变换把附件转换成文本，需要用 with_capabilities
显式声明变换器可接受的输入；变换前后分别按外层和内层能力校验。放在重试包装器内侧时
每次尝试准备一次，放在外侧时每次逻辑调用准备一次。资源解析、文件路径和异步 I/O
均由用户函数提供，包装器不启动后台任务。

## 运行参数与资源

```rust
let runtime = Runtime::builder(model)
    .runtime_tools(catalog)
    .store(store)
    .defaults(|run| run.stream(true).limits(limits))
    .build()?;

let request = RunRequest::new([Message::user("run")])
    .context(context)
    .options(ModelOptions { temperature: Some(0.4), ..Default::default() })
    .response_format(output.format());
let checkpoint = runtime.start(request)?.result().await?;
```

Runtime 保存共享模型、工具目录、存储和策略资源，以及新运行的参数默认值。
RunRequest 只覆盖显式设置的整字段：limits、model options、provider_tools、tool_choice、
response_format、stream；不会递归合并 JSON，也不会修改共享 Runtime。
`without_response_format()` 显式清空格式；`run_options(options)` 一次覆盖全部参数。

有效 RunOptions 在 start 时固化到 Checkpoint，随 wire、数据库核心记录保存。
continue 和 resume 都读取固化值，即便重新构造 Runtime 时使用不同默认值。
Store 提交和轨迹校验拒绝中途更改参数；需要不同参数时开始新运行。
资源实现仍由应用重建，Checkpoint 不序列化模型客户端、目录或策略闭包。

## 运行与结果

```rust
let checkpoint = zhir::runs::drive(invocation, |event| async move {
    handle_my_event(event).await
}).await?;

let report: Report = zhir::output::decode(&checkpoint)?;

let ticket = SuspensionTicket::from_checkpoint(&checkpoint)?;
let resumed = runtime.resume(
    ResumeRequest::from_ticket(ticket).message(Message::external("confirmed"))
).await?;
```

- drive 顺序消费事件，再返回结算结果。处理函数报错时取消并等待运行，返回
  DriveError::Observer { error, settled }，完整保留观察错误和实际运行结果。运行可能
  在取消到达前已经完成；不把该情况改写成取消。State::Failed 仍按现有契约返回 checkpoint。
- 慢处理函数仍面对有界、可丢弃的展示事件。需要完整 delta 观察时使用 ObservedModel。
  处理函数自身 I/O 的超时由用户负责；丢弃 drive future 会请求取消，但无法等待结算。
- output::decode 只接受 Completed 的纯文本 JSON，拼接文本片段后严格反序列化。
  Markdown、尾随说明和混合媒体均报错；未知字段是否报错由目标类型的 Serde 声明决定。
  它不设置 response_format、不校验外部 Schema、不修复回答或重试执行，不修改 checkpoint。
- `Runtime::resume(ResumeRequest)` 是统一异步入口。from_checkpoint 直接使用给定快照；
  from_ticket 从已配置 RunStore 读取 head。票据绑定 run、checkpoint id、revision 和完整
  Suspension；可用 Serde 持久化。即便新一轮重复使用相同 wait_id，旧票据也不能恢复它。
- 缺少存储、找不到 run、票据过期、选择器不匹配、终态及不合法的消息追加均报错。
  两个请求读取相同 head 后仍由唯一提交路径的 CAS 决定成功者，不自动重试冲突。

## 研发测试

在消费项目的 dev-dependencies 中引用 zhir-testing，不在生产依赖中引入：

```toml
[dev-dependencies]
zhir-testing = { path = "../zhir/crates/zhir-testing", features = ["http"] }
```

```rust
let scripted = Arc::new(ScriptedModel::responses([
    ModelResponse::text("done"),
]));
let runtime = Runtime::builder(scripted.clone()).build()?;
let checkpoint = runtime.start(RunRequest::new([Message::user("run")]))?.result().await?;
assert_eq!(scripted.requests().len(), 1);
assert_eq!(scripted.remaining(), 0);
```

ScriptStep 可声明增量、响应或错误，队列耗尽明确失败。并发调用按取得锁的次序消费
脚本；需要按请求数据匹配响应时使用 FunctionModel。RecordingModel 保存请求、run
和结果；未返回或被丢弃的调用保留 outcome: None。RecordingSink 可在指定的第 N 次
写入记录后报错。RecordingStore 记录成功等待到的提交，按 run/revision 验证轨迹，
重复的幂等提交核对一致性后计一次。

这些记录仅用于内存中的测试断言，不做持久化承诺；被丢弃的存储调用可能在数据库完成，
但来不及记录。当前 SDK 的正常依赖图不包含 zhir-testing。

`http` feature 提供可重用的本地 HTTP/SSE 传输夹具：

```rust
let fixture = HttpFixture::start([
    HttpReply::json(&my_response),
    HttpReply::sse(my_sse_frames).fragment_bytes(3),
]).await?;
let model = make_my_model(fixture.url())?;
// 调用模型……
let requests = fixture.finish().await?;
assert_eq!(requests[0].method, "POST");
assert_eq!(requests[0].json()?["my_option"], "expected");
```

HttpReply 支持状态码、Header、首包延迟、分片间隔和按字节截断。finish 等待消费全部脚本并
传播错误；每个请求交换有超时，未消费脚本也会失败。丢弃夹具或 finish future 会取消任务。
当前夹具是 HTTP/1.1、Content-Length 请求、逐请求关闭连接；不提供 TLS、WebSocket、
chunked 请求或生产服务行为。响应格式、能力语义与断言仍由测试作者定义。

完整的组合验证代码见 [developer_api.rs](../crates/zhir/tests/developer_api.rs)；
真实模型接入验证见 [ergonomics.rs](../crates/zhir/tests/scenario_scale/ergonomics.rs)。

## 输出契约与完整历史窗口

启用 `typed-output` 后，`JsonOutput<T>` 按 T 的反序列化规则生成 Draft 2020-12 Schema，
在构造时编译校验器。请求使用与本地校验相同的 Schema；供应商要求的格式转换仍由用户
协议扩展负责。错误区分 JSON 语法、Schema 路径和反序列化，不修改结果或自动重试。

```rust
let output = zhir::output::JsonOutput::<Report>::new("report")?;
let runtime = Runtime::builder(model)
    .defaults(|run| run.response_format(output.format()))
    .history_reducer(Arc::new(zhir::history::HistoryWindow::last_turns(12)?))
    .build()?;
let report = output.decode(&checkpoint)?;
```

HistoryWindow 的 N 轮包含当前用户轮次。系统消息保留原始相对顺序，一轮中的模型调用、
RuntimeToolCall、RuntimeTool 结果和外部回复一起保留。没有足够旧轮次时返回 None；
只在无 provider continuation 的 Planning 状态执行。`with_dependencies` 接收 checkpoint
与计划保留的起始消息下标，返回协议需要的更早下标；窗口自动向前扩展到完整用户轮次。
返回更晚下标报错。它不猜测令牌数，也不解析供应商的跨轮次依赖。

## 共享模型并发与工具目录视图

```rust
let model = Arc::new(zhir::models::ConcurrencyLimitedModel::new(model, 8)?);
let selected = zhir::runtime_tools::SelectedRuntimeTools::new(
    catalog,
    ["search", "read_file"],
)?;
let runtime = Runtime::builder(model).runtime_tools(Arc::new(selected)).build()?;
```

ConcurrencyLimitedModel 的克隆共享同一个信号量。许可覆盖完整 Model::invoke，包括所有
已等待的 delta sink 写入；取消、错误及丢弃 Future 均释放许可。排队和执行期间检查取消
与单调截止时间。需要限制实际模型尝试时，将重试器放在限制器外侧，使退避不占许可。

SelectedRuntimeTools 每次只打开一个底层目录快照，声明与绑定均来自该快照；目录后续
变化不影响当前调用。空选择合法，重复、空名称或目录中不存在的名称明确报错。绑定返回
的规格必须与快照相同。ProviderToolSpec 通过独立的 provider_tools 入口配置。

## ProviderTool 接入

执行归属在类型、请求、结果、事件和计数中保持一致：

| 层面 | 应用执行 | 服务端执行 |
| --- | --- | --- |
| 声明 | RuntimeToolSpec | ProviderToolSpec |
| 调用 | RuntimeToolCall | ProviderToolCall |
| 注册 | RuntimeToolRegistry / runtime_tools | ProviderTools 适配器 + provider_tools 声明 |
| 执行 | RuntimeTool::invoke，由 kernel 调度 | 服务端执行，框架记录与回放 |
| 增量 | ModelDelta::RuntimeTool、RuntimeToolProgress | ModelDelta::ProviderToolProgress |
| 原始协议 | ModelDelta::ProtocolEvent | ModelDelta::ProtocolEvent |
| 计数 | Metrics.runtime_tool_calls | 保留在模型输出与调用记录中 |

`zhir::models::provider_tools::ProviderToolAdapter` 是编解码会话接口，没有 invoke。
用户实现 identity、encode、decode；ProviderOutput 构建的结果可直接使用默认 replay，
自定义回放、choice 和 event 按需实现，然后在每次
模型调用的扩展工厂中创建 ProviderTools 并 register。注册适配器不自动启用能力：
RunRequest::provider_tools 或 RuntimeBuilder::defaults 还需要显式传入 ProviderToolSpec。

声明中的 provider/name 是用户选择的能力身份，不受传输协议名称限制；options 是用户
载荷，由适配器解释。编码结果必须是原生声明对象。显式选择的原生格式由 choice 返回，
不根据 name 猜测。decode 可以返回 None（未认领）、Some(empty)（消费关联结果项）或
本能力的 ProviderToolCall；调用保持响应顺序，身份不能越过该适配器注册的范围。

扩展按以下位置接入标准 HTTP/SSE 流程：

- encode_provider_tool / encode_provider_choice：在标准请求编码阶段运行。
- decode_output_item：在单个输出项的标准解码前运行，同时提供完整原生响应。
- encode_provider_history：对已记录的 ProviderToolCall 生成当前请求的回放项。
- decode_event：在标准流累积前处理原生帧；框架同时保留原始 ProtocolEvent。
- encode_request / decode_response：仍可对完整请求或响应进行用户定义的转换。

ExtensionChain 的整请求、事件和整响应 hook 按顺序组合。声明、选择、输出项与回放项
使用唯一归属规则：多个扩展同时认领同一项直接报错。ProviderTools 在每次模型调用内
维护独立状态，拒绝未启用能力的输出；事件只能生成本适配器身份的 ProviderToolProgress。

未映射的执行请求和未知顶层输出项明确报错。框架不会根据类型名称后缀或原生 completed
状态推断执行归属。应用执行的特殊协议项由用户的 ProtocolExtension 映射为
RuntimeToolCall，结果仍通过唯一的 RuntimeTool 执行与提交路径处理。

完整消费者实现见 [provider_fixture](../crates/zhir/tests/provider_fixture/mod.rs)，
执行、混合工具及回放测试见 [provider_integration](../crates/zhir/tests/provider_integration.rs)。
这些具体能力映射仅存在于消费者测试中。

## 产物存取与结果查询

`zhir::core::artifact::ArtifactStore` 提供异步 put/get。存储实现由用户提供：put 成功返回
时引用应已持久化；相同 key 和内容应返回相同引用。应用负责回收未提交响应遗留的产物。
框架不会替用户部署存储服务。

`ArtifactModel` 包装任意 Model，在调用前解析类型化 MediaSource::Artifact，在调用后
保存规范化输出中的 Inline 产物。保存 key 根据 run 身份、媒体类型和内容生成，同一响应
的相同内容只写一次。读取在本次请求内缓存，并检查引用与内容的媒体类型一致。

```rust
let model = ArtifactModel::new(model, artifact_store);

for record in zhir::output::provider_calls(&checkpoint) {
    println!("{}:{} {}", record.message_index, record.output_index, record.call.name);
    // record.call.status / output / data 可直接按标准 Rust 迭代器筛选。
}
```

在 ProviderToolAdapter::decode 中同时构建规范化输出和原生回放：

```rust
let output = ProviderOutput::new(provider, name, id, status)
    .native(item.clone())
    .image("/result", "image/png")?
    .finish()?;
Ok(Some(vec![output]))
```

image 从当前原生条目的本地 JSON Pointer 读取 base64，并记录媒体关联。通用 media
接收 `FnOnce(MediaSource) -> Content`，可构建音频、视频和文件。反复调用 native 可追加
成对或多项回放，随后声明的媒体始终关联最近追加的原生条目。普通 content 可附加文本和 URL。

规范化产物变为 MediaSource::Artifact，原生字段变为
`{"$zhir_artifact":{"id":"...","mime_type":"..."}}`。唯一原生载荷在 call.data 的
`$zhir_provider_replay` 中，内部保存 items 与本地媒体关联；应用使用
`ProviderOutput::replay(call)` 读取原生项。绑定必须唯一且内容一致，未解析的引用在模型 I/O 前加载。

原始快照仍包含未绑定的相同载荷时，保存流程报错，避免 checkpoint 继续携带一份内联
产物。普通业务参数中名为 kind/id 的对象不会被当成产物。部分流事件只用于观察；它们
不会由 ArtifactModel 自动升级为最终输出。存储失败返回原错误，kernel 保留最后一个
成功提交的 checkpoint；存储错误以 RunError.last_checkpoint 提供恢复位置。

ProviderToolRecord 的 message_index/output_index 只在对应 checkpoint 内有效；原生
call id 可能在不同模型轮次重复。查询保留这些位置，不把不同轮次的同名调用合并。

原生响应中被 ProviderTool 认领的位置替换为 `{"$zhir_provider_calls":[call_id,...]}`，
无需另存一份同样的媒体载荷。回放根据 call id 找到同一响应内的规范化调用，交给用户适配器；
重排规范化 output 不影响关联。成对消费项使用空数组标记，不重复回放。缺失调用或重复位置
报错；原生 id/call_id/token 等身份字段完全由用户解码器解释。消费项目改写整个响应时，也须
保留这些关联的完整性；普通未认领内容和响应级元数据保持原样。

ProviderToolCall.output 是服务端产物，独立于模型的原生输入模态。模型只支持文本输入时，
用户适配器仍可以返回音频、视频和文件，并按服务端引用回放。普通 User/Assistant 内容
直接携带这些媒体时，仍执行模型输入能力校验。
