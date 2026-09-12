# zhir

zhir 是可嵌入的 Rust Agent SDK，也是后续独立产品的公共基础。

它提供模型与工具循环、流式事件、暂停恢复、运行中控制、原子 checkpoint、
模型适配器、四种存储和官方工具。执行由宿主的 Tokio 运行环境承载。

## 组件

| Crate | 职责 |
| --- | --- |
| `zhir-core` | 公共类型、扩展 trait、原生 wire DTO |
| `zhir-kernel` | 唯一执行引擎、调度、控制、提交、诊断 |
| `zhir-models` | 函数式模型、异步请求变换、扩展组合；可选 HTTP 协议 |
| `zhir-tools` | 工具注册、函数与强类型适配、校验、重试、熔断 |
| `zhir-builtins` | 文件、Shell、交互、子 Agent 工具 |
| `zhir-storage` | Memory、SQLite、MySQL、Redis |
| `zhir` | SDK 统一入口与精选导出 |
| `zhir-testing` | 供研发测试使用的脚本模型、记录、HTTP/SSE 夹具与故障注入，只作为开发依赖 |

工具协议只定义在 core。tools 管理工具的装配，builtins 提供具体实现，kernel
管理执行生命周期。模型、工具装配和存储之间不互相依赖。

## 使用

本仓库初始版本为 0.1.0，尚未发布到 crates.io。本地产品可以使用路径依赖：

```toml
[dependencies]
zhir = { path = "../zhir/crates/zhir", features = ["openai-chat", "tools", "sqlite"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,no_run
use std::sync::Arc;
use zhir::{RunRequest, Runtime, message::Message, RunOutcome};
use zhir::models::{ModelConfig, openai};
use zhir::stores::sqlite::SqliteRunStore;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let model = openai::chat::model(ModelConfig::new(
    "https://api.openai.com/v1",
    std::env::var("OPENAI_API_KEY")?,
    std::env::var("OPENAI_MODEL")?,
))?;
let store = SqliteRunStore::connect("sqlite://runs.db?mode=rwc").await?;
let runtime = Runtime::builder(Arc::new(model))
    .store(Arc::new(store))
    .defaults(|run| run.stream(true))
    .build()?;
let mut run = runtime.start(RunRequest::new([Message::user("Hello")]))?;
let result = run.result().await?;
if let RunOutcome::Completed(content) = result.outcome() {
    for part in content {
        if let Some(text) = part.as_text() { print!("{text}"); }
    }
}
# Ok(())
# }
```

默认 SDK 仅启用 core 和 kernel 两个执行组件。可选 feature 为 `tools`、`typed-tools`、`typed-output`、`models`、`filesystem`、`shell`、
`interaction`、`agent`、`openai-chat`、`openai-responses`、`anthropic`、`memory`、
`sqlite`、`mysql`、`redis`、`artifacts-filesystem`。

`models` 提供通用模型组件，不引入 HTTP 客户端；具体协议 feature 自动启用它。
`typed-tools` 从 Rust 类型生成工具 Schema；`typed-output` 提供请求与本地校验共用的
`JsonOutput<T>`。完整用法与扩展方法见[研发接入指南](docs/developer-api.md)。

应用执行的能力统一使用 `RuntimeTool` 类型族；服务端能力使用 `ProviderToolSpec`、
`ProviderToolCall` 和用户实现的 `ProviderToolAdapter`。具体能力声明、事件解析及回放
由消费项目实现。SDK 提供适配器组合、完整历史窗口、共享模型并发、目录快照选择和产物存取接口。

完整示例在 [SDK examples](crates/zhir/examples)。配置、凭据、存储路径和客户端
生命周期由调用方提供；SDK 不读取全局配置，不自动启动服务。

无需模型凭据即可运行自定义工具与暂停恢复示例：

```sh
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --example resume --features interaction,memory
```

`chat` 示例需要 `OPENAI_API_KEY` 和 `OPENAI_MODEL`，运行时启用 `openai-chat,sqlite`。

## 开发与验证

使用 `rust-toolchain.toml` 指定的工具链，通过工作区 `Cargo.lock` 固定依赖。

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
```

独立 feature、契约、真实数据库、模型接入及包验证入口见[测试说明](docs/testing.md)。

[架构与职责](docs/architecture.md) · [研发接入](docs/developer-api.md) ·
[运行契约](contracts/v1/behavior/runtime.md) · [测试说明](docs/testing.md)
