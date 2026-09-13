# zhir

zhir 是可嵌入的 Rust Agent SDK，为研发平台提供模型会话、异步操作、资源流、运行控制和持久化恢复。宿主提供 Tokio 运行环境、模型客户端、凭据、工具与存储。

当前工作区版本为 **0.2.0**，checkpoint 和存储格式为 **v2**。这是一次直接替换公共契约的重构；使用新的数据库、Redis namespace 和资源目录。

## 组件

| Crate | 职责 |
| --- | --- |
| `zhir-core` | 公共值、扩展 trait、会话/操作/资源/凭据契约、wire DTO |
| `zhir-policies` | 能力协商、重试策略、退避与历史窗口；只依赖 core |
| `zhir-kernel` | 唯一执行状态机、调度、控制、outbox、checkpoint 提交 |
| `zhir-models` | 会话装饰器、资源解析、凭据刷新及可选 HTTP 协议 |
| `zhir-tools` | 工具注册、结构化/自由文本输入、类型适配、执行结果校验 |
| `zhir-builtins` | 文件、Shell、交互工具，以及基于 operation 的子 Agent |
| `zhir-storage` | Memory、SQLite、MySQL、Redis 运行存储；内存/文件资源存储 |
| `zhir` | SDK facade 与便利 API |
| `zhir-testing` | 测试模型、HTTP/SSE 夹具、记录、轨迹验收与基准；仅用于研发测试 |

## 使用

在本地产品中使用路径依赖：

```toml
[dependencies]
zhir = { path = "../zhir/crates/zhir", features = ["openai-chat", "tools", "sqlite"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use std::sync::Arc;
use zhir::{RunRequest, Runtime, RunOutcome, message::Message};
use zhir::models::{ModelConfig, credentials::StaticCredential, openai};
use zhir::stores::sqlite::SqliteRunStore;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let model = openai::chat::model(ModelConfig::new(
    "https://api.openai.com/v1",
    Arc::new(StaticCredential::new("Bearer", std::env::var("OPENAI_API_KEY")?)),
    std::env::var("OPENAI_MODEL")?,
))?;
let store = SqliteRunStore::connect("sqlite://runs-v2.db?mode=rwc").await?;
let runtime = Runtime::builder(Arc::new(model))
    .store(Arc::new(store))
    .defaults(|run| run.stream(true))
    .build()?;
let result = runtime.start(RunRequest::new([Message::user("Hello")]))?
    .result().await?;
if let RunOutcome::Completed(content) = result.outcome() {
    for part in content {
        if let Some(text) = part.as_text() { print!("{text}"); }
    }
}
# Ok(())
# }
```

默认 feature 为空，只引入 core 与 kernel。按需启用 `policies`、`tools`、`typed-tools`、`typed-output`、`models`、`filesystem`、`shell`、`interaction`、`agent`、`agent-runtime`、`openai-chat`、`openai-responses`、`anthropic`、`memory`、`sqlite`、`mysql`、`redis`、`resources-filesystem`。

模型统一实现 `Model::open_session`。应用工具统一实现 `RuntimeTool::start/recover`，返回最终结果或可恢复的 `OperationHandle`。服务端工具仍由模型适配器执行，kernel 记录其 operation；两种执行归属不会混淆。

`RequestProfile` 区分必须满足与偏好，资源使用要求保存在 `ResourceUsage`。协商结果与服务端实际确认分开保存，没有确认的字段保持 `Unknown`。双向媒体使用独立、有界的分块通道。

HTTP 适配器提供 Chat、Responses、Messages 的轮次协议。原生双向会话、具体供应商视频/语音任务和 OAuth 登录流程由接入方实现对应接口。测试中的供应商场景验证接入机制，不代表已经集成某个最新模型的全部线上能力。

无需凭据即可运行示例：

```sh
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --no-default-features --example resume --features models,interaction,memory
```

## 开发与验证

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
```

验收、故障注入、trace 校验和基准只放在不发布的 `zhir-testing` 与 `conformance`。独立 feature、契约、真实数据库和包验证方法见[测试说明](docs/testing.md)。

[架构与职责](docs/architecture.md) · [研发接入](docs/developer-api.md) · [运行契约](contracts/v2/behavior/runtime.md) · [测试说明](docs/testing.md)
