# 测试与验证

使用 rust-toolchain.toml 指定的 Rust 1.95 与已提交的 Cargo.lock。验收代码、故障注入、
trace 校验、合成供应商、消费者验收与基准只允许位于 `crates/zhir-testing` 和
`conformance`。所有生产包的 src/tests/benches 中不放验收代码，也不依赖测试包；依赖边界测试检查这一规则。
日志和报告放在根目录被忽略的 `test-results/` 或 CI artifacts。Python 开发脚本只使用 uv。

## 必需检查

在仓库根目录运行：

```sh
mkdir -p test-results
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
cargo run -p zhir-conformance --bin zhir-conformance --locked
cargo run -p zhir-conformance --bin schemas --locked -- --check
```

修改公开 DTO 后先执行 `cargo run -p zhir-conformance --bin schemas`，提交生成的
`contracts/v3/schemas/`，再运行 check。契约 runner 包含 41 个当前 v3 JSON 案例，
覆盖状态、资源、profile、工具结算和运行限制。原生会话时序及故障验收由 Rust 测试覆盖，
不能用 JSON 案例数量或通过率代替这部分证据。

| 测试入口（均位于测试模块） | 验收重点 |
| --- | --- |
| `conformance/tests/dependencies.rs` | 生产依赖图、feature 边界、验收代码位置 |
| `core_values`、`core_boundaries` | 值、history、wire、运行参数、目录与绑定 |
| `session_runtime` | 原生会话早发工具、provider 任务、等待恢复、媒体封存 |
| `session_recovery` | 未确认 outbox 不重发、竞争恢复 CAS、跨轮 provider 完成、重复/冲突完成、双向流、中断及 EndInput |
| `runtime_deadlines` | catalog/model 建立阶段截止时间、未确认 commit 超时 |
| `refactoring` | provider operation 结算后的跨轮 replay 与 wire 往返、4096 项历史合并顺序、虚拟时钟下有界并发取消及统一退出预算 |
| `minimax_tts`（feature `minimax`） | 生产 WebSocket TTS：分句封存、中断、尾音、背压、凭据刷新与连接生命周期；ignored 测试访问真实服务 |
| `models_*` | 请求与流协议、Unicode/分片、回放、装饰器、资源预算、会话资源输入 |
| `profiles_credentials` | required/preferred、fast/original 映射、Unknown、401 刷新与账号头 |
| `tools_*`、`policies_*` | 工具 Schema、Active 最终校验、重试/熔断与历史策略 |
| `builtins_*` | 文件、Shell、交互、子任务限流/恢复/脱离/持久化取消 |
| `provider_integration` | 自定义 provider 执行归属、媒体绑定、原生回放与并发隔离 |
| `developer_api`、`consumer_six`、`consumer_ten`、`convenience` | 消费者组合、票据、参数隔离、选择、强类型上下文与输出 |
| `scenario_scale`、`http_fixture` | 本地并发 HTTP/SSE、密集事件、RPC 工作流及传输故障 |
| `storage_stores` | 四种存储共享的原子提交、冲突、截止时间、历史与 96 个等待操作恢复 |

## 独立 feature、示例与发布包

完整 feature 矩阵以 [CI](../.github/workflows/ci.yml) 为准。逐个启用，不能用 all-features
成功替代这些检查：

```sh
cargo check -p zhir-core --no-default-features --locked
cargo check -p zhir-testing --no-default-features --locked
cargo check -p zhir-testing --no-default-features --features http --locked
cargo check -p zhir-models --no-default-features --features minimax --locked
cargo test -p zhir-testing --no-default-features --features minimax --test minimax_tts --locked
cargo check -p zhir --no-default-features --locked
for feature in policies tools typed-tools typed-output models filesystem shell interaction agent agent-runtime openai-chat openai-responses anthropic minimax memory sqlite mysql redis resources-filesystem; do
  cargo check -p zhir --no-default-features --features "$feature" --locked || exit 1
done
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --no-default-features --example resume --features models,interaction,memory
uv run conformance/package.py
```

包验证脚本按 allowlist 只选择 8 个生产包，完成 archive 内源码编译，并检查归档文件、
manifest 和 lockfile 均没有测试代码或测试包依赖。`zhir-testing` 与 `conformance` 都不发布。
`publish = false` 不代替打包选择；不要用不带排除项的 `cargo package --workspace`。
脚本每次生成独立临时 target-dir，防止同版本 registry 复用旧源码；结束后自动清理。
无网络验证可使用 `uv run conformance/package.py --offline`；新增可选协议后使用
`uv run conformance/package.py --offline --all-features` 同时编译归档中的可选实现。
消费者最小 feature 测试运行在 `zhir-testing`，同名 feature 转发到 SDK；CI 保留这些检查。
示例仍在 `zhir/examples`，生产归档仅允许 src、examples、manifest、README 和 LICENSE（以及 Cargo 生成的元数据）。

基准入口全部放在测试 crate：

```sh
cargo bench -p zhir-testing --bench history
cargo bench -p zhir-testing --bench trace
cargo test -p zhir-testing --all-features --release --test models_streaming stream_append_scale -- --ignored --nocapture
```

这些入口观察 history 增长、trace 校验与流式组装成本，打印规模/耗时，不用脆弱的耗时
阈值作为普通回归断言。基准数据不是吞吐量或延迟保证。

## 真实存储集成

使用独立测试数据库；Memory 与 SQLite 在普通 workspace 测试中运行。MySQL、Redis
仅在显式提供测试环境时运行：

```sh
export ZHIR_TEST_MYSQL_URL='mysql://root:password@127.0.0.1:3306/zhir_test'
export ZHIR_TEST_REDIS_URL='redis://127.0.0.1:6379/'
cargo test -p zhir-testing --all-features --test storage_stores -- --ignored
```

CI 使用 MySQL 8.4 与 Redis 7。测试生成独立 run ID 和 Redis namespace，校验历史
追加/替换、幂等写、错误 parent/delta/options、过期写、读取重建与原生 operation 恢复。
SQL 数据库必须是当前格式或空库，Redis namespace 必须是当前格式或空 namespace。
这不等于数据库故障切换、网络分区、跨区域复制或长期运行压测。

## 外部服务验证范围

普通测试只访问本地夹具；带 ignored 的模型测试需要凭据并会产生费用。
现有线上测试使用 DEEPSEEK_API_KEY，测试自己的端点扩展映射；它们不是 OpenAI Astra、
Codex OAuth 或 MiniMax 视频的线上验收。MiniMax 语音由独立的显式测试验证。

| target | ignored 测试 | 报告路径环境变量 |
| --- | --- | --- |
| `live_protocols` | `live_protocol_matrix` | `DEEPSEEK_LIVE_REPORT` |
| `extensibility` | `live_consumer_capability_audit` | `ZHIR_LIVE_AUDIT_REPORT` |
| `scenario_scale` | `live_scenario_scale` | `ZHIR_SCALE_REPORT` |
| `scenario_scale` | `live_developer_api` | `ZHIR_DEVELOPER_REPORT` |

例如，凭据由调用环境提供后：

```sh
ZHIR_DEVELOPER_REPORT="$PWD/test-results/developer-live.json" \
  cargo test -p zhir-testing --all-features --test scenario_scale live_developer_api -- --ignored --nocapture
```

MiniMax 的复现命令、cc-switch 启动脚本和音频证据说明见
[zhir-testing README](../crates/zhir-testing/README.md#minimax-tts-integration-tests)。
真实 TTS 测试验证连续输入收尾及同任务中断后继续；不验证断线恢复或音频输入。

五类需求的本地验收分别验证：丰富模型/服务端工具扩展、跨服务 runtime operation、
OAuth 风格刷新与账号头、显式低延迟/原图要求、原生音视频双向流。它们证明 SDK 接口与
执行语义，不证明任何账号的 token plan 权益、具体模型的最新全部能力或媒体生成质量。
实际登录流程、其他 WebSocket/WebRTC 协议、供应商后台任务 API 和模型目录由相应接入层补充。

GPT-Live acceptance uses an in-process Rust WebRTC peer for SDP, DataChannel,
UTF-8 delegation context and bidirectional Opus RTP. It runs without credentials:

```sh
cargo test -p zhir-testing --no-default-features --features openai-live --test gpt_live
cargo test -p zhir-testing --test session_semantics
cargo check -p zhir --no-default-features --example gpt_live --features openai-live,memory
```

The explicit `codex_subscription_through_native_runtime` ignored test creates one
voice session with a locally supplied Codex auth file. It checks actual received
Opus packets, conversation history, completion and archived media. The optional proxy
is injected into the signaling client; audio uses WebRTC networking.

```sh
ZHIR_LIVE_AUTH_JSON=/path/to/codex/auth.json \
  cargo test -p zhir-testing --features openai-live --test gpt_live \
  codex_subscription -- --ignored --nocapture
```

Set `ZHIR_LIVE_PROXY` only when signaling needs a proxy. Tests never print credentials.
Online success establishes the tested account/endpoint combination, not entitlement
for every account, backend delegation correctness, microphone quality or recovery.
