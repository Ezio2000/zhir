# 测试与验证

使用 rust-toolchain.toml 指定的 Rust 1.95 与已提交的 Cargo.lock。验收代码、故障注入、
trace 校验、合成供应商、消费者验收与基准只允许位于 `crates/zhir-testing` 和
`conformance`。所有生产包的 src/tests/benches 中不放验收代码，也不依赖测试包；依赖边界测试检查这一规则。
日志和报告放在根目录被忽略的 `test-results/` 或 CI artifacts。Python 开发脚本只使用 uv。

## 本地：验证本次变更

本地运行新增或修改的测试，以及本次变更直接影响的既有回归；按测试 target、过滤条件和
最少所需 features 选择范围。测试通过后结束本地验证，不默认追加全 workspace 测试、
全量 Clippy、烟测、feature 矩阵、完整 conformance、存储/服务集成套件或发布包验证。
这些完整检查由 CI 执行，只有用户明确要求时才在本地运行。

- Rust 修改检查格式；必要时对受影响 package/target 做编译或 Clippy 检查。
- API 修改同步更新受影响调用方，按需编译这些调用方，不自动展开全部 feature 组合。
- 测试过滤后应确认实际执行的用例；`0 passed` 不能作为验证通过的依据。
- `tests/` 中的定向回归和进程内夹具可以用于本地验证；目录名称不决定验证范围。
- 纯文档修改只检查改动与文档一致性，不运行 Rust 测试。
- 分别报告本地验证和 CI 状态；CI 尚未运行时明确说明，不把它算成本地验证失败。
  排查 CI 失败先读日志，本地复现仍遵循上述范围规则。

例如，修改观察 delta 的 generation 传递时，在仓库根目录运行：

```sh
cargo fmt --all --check
cargo test -p zhir-testing --no-default-features --features models,tools,memory,interaction \
  --test session_runtime observer_deltas_preserve_generation_identity_and_session_scope \
  --locked -- --exact
```

修改公开 DTO 时，本地更新并核对生成产物：

```sh
cargo run -p zhir-conformance --bin schemas --locked
cargo run -p zhir-conformance --bin schemas --locked -- --check
```

提交生成的 `contracts/v5/schemas/`。其余变更按下面的测试入口选择直接相关的回归，
不要把入口表当作每次本地都要执行的清单。

`zhir-testing` 的 HTTP、WebSocket、WebRTC、SQLite、MySQL、Redis 依赖按 feature
启用，内存运行时测试不编译这些依赖。测试 target 的最少 feature 声明在
`crates/zhir-testing/Cargo.toml` 的 `required-features`；直接指定 target 却缺少 feature
时 Cargo 会报错，避免整个文件被 cfg 跳过后得到 `0 passed`。
同一 target 内的可选用例仍按各自 feature 启用，例如 `storage_stores` 的数据库用例。
`provider_integration` 包含文件资源持久化用例，还需要 `resources-filesystem`。

## CI：完整验证

[CI](../.github/workflows/ci.yml) 在 PR 和 main 推送时运行完整检查。以下命令属于 CI
验证范围，不是本地每次改动的必跑流程：

```sh
mkdir -p test-results
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
cargo run -p zhir-conformance --bin zhir-conformance --locked
cargo run -p zhir-conformance --bin schemas --locked -- --check
```

契约 runner 包含 41 个当前 v5 JSON 案例，
覆盖状态、资源、profile、工具结算和运行限制。原生会话时序及故障验收由 Rust 测试覆盖，
不能用 JSON 案例数量或通过率代替这部分证据。

| 测试入口（均位于测试模块） | 验收重点 |
| --- | --- |
| `conformance/tests/dependencies.rs` | 生产依赖图、feature 边界、定向测试不引入未选用的重依赖、接入包不直接依赖 webrtc/tokio-tungstenite、验收代码位置 |
| `core_values`、`core_boundaries` | 值、history（追加后共享已有条目）、wire、运行参数、目录与绑定 |
| `session_runtime` | 原生会话早发工具、provider 任务、等待恢复、媒体封存；观察 delta 跨生成保留身份及会话级 None |
| `session_semantics` | 委派与会话语义、委派恢复失败保持 Unknown、流归档；2000 条流不重扫归档链、慢存储下排队包合并为一段且提交少于包数、宿主媒体输入在包数上限处阻塞 |
| `session_recovery` | 未确认 outbox 不重发、竞争恢复 CAS、跨轮 provider 完成、重复/冲突完成、双向流、中断及 SealUserInput；本地投影在建立中或 Generate 发出前崩溃后可续跑，已发出的生成需 AbandonGeneration，处置不符即失败，被放弃这一代的 provider 操作须先结算、之前各代的 provider 操作随重新生成继续，批内 profile 协商覆盖尚未提交的输出，连接被拒以 `http_connect` 失败 |
| `runtime_deadlines` | catalog/model 建立阶段截止时间、未确认 commit 超时；虚拟时钟下空闲运行零轮询、取消无需推进时间即结算、截止时间由定时器触发 |
| `runtime_operations` | 工具按发出顺序准入、纯文本一轮（9 次）与一次工具调用一轮（15 次）的提交次数上限、串行屏障与并行分组、审批期间不再准入、start 错误结算、取消运行中工具、OperationChanged 完整序列、恢复后工具从序号 0 重放与同序号冲突 |
| `refactoring` | provider operation 结算后的跨轮 replay 与 wire 往返、4096 项历史合并顺序、虚拟时钟下有界并发取消及统一退出预算 |
| `minimax_tts`（feature `minimax`） | 生产 WebSocket TTS：分句封存、中断、尾音、背压、凭据刷新与连接生命周期、运行写入的全部资源都能从最终 checkpoint 到达；ignored 测试访问真实服务 |
| `webrtc_driver`（feature `webrtc`） | 生产骨架配确定性传输与非 Live 适配器：无 Peer 命令、主动建连、排水期间确认与期限、发送顺序、媒体预算与关闭边界；TURN 服务器的凭据传入 WebRTC 栈 |
| `models_*` | 请求与流协议、Unicode/分片、回放、装饰器、资源预算、会话资源输入；HTTP 429 + Retry-After 重试、400 不重试、5xx 耗尽、响应体开始后不重试、连接被拒、截止时间约束与共享请求限流 |
| `contract_review` | 检查点 schema、最新输入完成条件、恢复绑定、小栈历史结算，以及 exchange 回调即时发出 delta 时的确认/开始顺序和有界队列取消 |
| `models_decorators` | 会话包装器转发与绑定；独立保留 control、events、media.input 或 media.output 时，会话并发额度持续占用，最后一个端口释放后才允许新会话 |
| `profiles_credentials` | required/preferred、fast/original 映射、Unknown、401 刷新与账号头 |
| `tools_*`、`policies_*` | 工具 Schema、Active 最终校验、重试/熔断与历史策略 |
| `builtins_*` | 文件、Shell（逃逸子进程的输出宽限）、交互、输入错误码、子任务并发/限流/恢复/脱离/持久化取消 |
| `provider_integration` | 自定义 provider 执行归属、媒体绑定、原生回放与并发隔离 |
| `developer_api`、`consumer_six`、`consumer_ten`、`convenience` | 消费者组合、票据、参数隔离、选择、强类型上下文与输出、资源删除、文件资源分目录与格式标记拒绝 |
| `scenario_scale`、`http_fixture` | 本地并发 HTTP/SSE、密集事件、RPC 工作流及传输故障 |
| `storage_stores` | 四种存储共享的原子提交、冲突、截止时间、历史与 96 个等待操作恢复；重写后只留当前一代历史、删除运行后无残留、`reachable` 覆盖历史/完成内容/活动游标/归档链、内容直接引用媒体节点时仍遍历其数据与前驱，并在节点缺失时失败；两个 SQLite 实例共享文件并发写全部提交、竞争首写一成一冲突 |

## CI：独立 feature、示例与发布包

完整 feature 矩阵以 [CI](../.github/workflows/ci.yml) 为准，由 CI 逐个启用，不能用
all-features 成功替代这些检查。下面列出 CI 使用的独立构建、协议集成与示例烟测入口：

```sh
cargo check -p zhir-core --no-default-features --locked
cargo check -p zhir-testing --no-default-features --locked
cargo check -p zhir-testing --no-default-features --features http --locked
cargo check -p zhir-models --no-default-features --features websocket --locked
cargo check -p zhir-models --no-default-features --features webrtc --locked
cargo check -p zhir-minimax --no-default-features --locked
cargo check -p zhir-openai --no-default-features --locked
cargo test -p zhir-testing --no-default-features --features webrtc --test webrtc_driver --locked
cargo test -p zhir-testing --no-default-features --features minimax --test minimax_tts --locked
cargo check -p zhir --no-default-features --locked
for feature in policies tools typed-tools typed-output models filesystem shell interaction agent agent-runtime openai-chat openai-responses anthropic memory sqlite mysql redis resources-filesystem; do
  cargo check -p zhir --no-default-features --features "$feature" --locked || exit 1
done
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --no-default-features --example resume --features models,interaction,memory
uv run conformance/package.py --all-features
```

包验证脚本按 allowlist 只选择 10 个生产包，完成 archive 内源码编译，并检查归档文件、
manifest 和 lockfile 均没有测试代码或测试包依赖。`zhir-testing` 与 `conformance` 都不发布。
`publish = false` 不代替打包选择；不要用不带排除项的 `cargo package --workspace`。
脚本每次生成独立临时 target-dir，防止同版本 registry 复用旧源码；结束后自动清理。
显式要求离线包验证时使用 `uv run conformance/package.py --offline --all-features`，
同样编译归档中的可选实现。
消费者最小 feature 测试运行在 `zhir-testing`；SDK feature 转发到门面，
`minimax` 和 `openai-live` 分别启用独立接入包。CI 保留这些检查。
SDK 示例在 `zhir/examples`，供应商示例在各自独立包的 `examples`，生产归档仅允许 src、examples、manifest、README 和 LICENSE（以及 Cargo 生成的元数据）。

## 按需性能验证

基准入口全部放在测试 crate。当前常规 CI 不自动运行这些入口；它们仅用于明确要求的
性能验证，不属于日常本地变更检查：

```sh
cargo bench -p zhir-testing --bench history
cargo bench -p zhir-testing --bench trace
cargo test -p zhir-testing --all-features --release --test models_streaming stream_append_scale -- --ignored --nocapture
```

这些入口观察 history 增长（条数与单条大小两个维度）、trace 校验与流式组装成本，打印规模/耗时，不用脆弱的耗时
阈值作为普通回归断言。基准数据不是吞吐量或延迟保证。

## CI：真实存储集成

存储集成套件由 CI 执行，使用独立测试数据库；Memory 与 SQLite 包含在 CI 的 workspace
测试中，MySQL、Redis 由 `stores` job 提供服务并显式运行 ignored 用例。下面是该 job 的
环境与命令示意；用户明确要求本地复现时才在本地配置这些环境：

```sh
export ZHIR_TEST_MYSQL_URL='mysql://root:password@127.0.0.1:3306/zhir_test'
export ZHIR_TEST_REDIS_URL='redis://127.0.0.1:6379/'
cargo test -p zhir-testing --no-default-features --features mysql,redis --test storage_stores --locked -- --ignored
```

CI 使用 MySQL 8.4 与 Redis 7。测试生成独立 run ID 和 Redis namespace，校验历史
追加/替换、替换后旧一代清除、幂等写、错误 parent/delta/options、过期写、读取重建、
删除运行与原生 operation 恢复。
SQL 数据库必须是当前格式或空库，Redis namespace 必须是当前格式或空 namespace。
这不等于数据库故障切换、网络分区、跨区域复制或长期运行压测。

## 外部服务验证范围

普通 CI 使用进程内或回环网络夹具；带 ignored 的真实模型测试需要凭据并会产生费用，
当前 CI 不自动执行这些用例。下面的命令是显式授权专项验收的入口，不属于日常本地验证。
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
真实 TTS 测试对六种格式运行 18 个收尾、flush 后继续和同任务打断案例，验证持久化后交付与格式标注，
并通过主机 ffmpeg 严格解码每条完整流。MP3 案例同时发送混音、情绪、归一化、公式、效果、字幕和连续推理配置；
验证参数被接受与音频可解码，不等于验证主观音质或服务端未返回的字幕。测试不覆盖断线恢复或音频输入。

五类需求的本地验收分别验证：丰富模型/服务端工具扩展、跨服务 runtime operation、
OAuth 风格刷新与账号头、显式低延迟/原图要求、原生音视频双向流。它们证明 SDK 接口与
执行语义，不证明任何账号的 token plan 权益、具体模型的最新全部能力或媒体生成质量。
实际登录流程、其他 WebSocket/WebRTC 协议、供应商后台任务 API 和模型目录由相应接入层补充。

GPT-Live acceptance uses an in-process Rust WebRTC peer for SDP, DataChannel,
UTF-8 delegation context and bidirectional Opus RTP. It runs without credentials:

```sh
cargo test -p zhir-testing --no-default-features --features openai-live --test gpt_live
cargo test -p zhir-testing --test session_semantics
cargo check -p zhir-openai --no-default-features --example gpt_live
```

The explicit `codex_subscription_through_native_runtime` ignored test creates one
voice session with a locally supplied Codex auth file. It checks actual received
Opus packets, acknowledged input pause/resume, conversation history, completion and archived media. The optional proxy
is injected into the signaling client; audio uses WebRTC networking.

```sh
ZHIR_LIVE_AUTH_JSON=/path/to/codex/auth.json \
  cargo test -p zhir-testing --features openai-live --test gpt_live \
  codex_subscription -- --ignored --nocapture
```

Set `ZHIR_LIVE_PROXY` only when signaling needs a proxy. Tests never print credentials.
Online success establishes the tested account/endpoint combination, not entitlement
for every account, backend delegation correctness, microphone quality or process-restart recovery.


`ZHIR_LIVE_INPUT_PACKETS` may point to a JSON array of raw 20 ms Opus byte packets
for a spoken fixture. The fixture must be shorter than 2.5 seconds; the test waits
for the Ready event, supplies a silence lead-in, then sends speech at
20 ms intervals. The current fixture is MiniMax saying “这是会话测试”, encoded by
host ffmpeg as 48 kHz stereo Opus. This additionally checks a remote user transcript;
it does not claim microphone or human barge-in coverage. Reports belong in
`test-results/native-support/`, and complete MiniMax sentence containers can be decoded
individually with ffmpeg.

Local regressions cover delayed flush receipts, responsive heartbeat without task
acknowledgement, blocked media with control progress, and old-epoch packets arriving
between two current-epoch media-manifest commits (`media_interrupt`). Live control
pressure tests also verify that closure preserves all received RTP before the end marker.


To encode one complete spoken MiniMax sentence and run the spoken Live check:

```sh
uv run --managed-python crates/zhir-testing/scripts/live_input.py test-results/minimax-tts/mp3-drain-epoch-0-sentence-1.mp3
ZHIR_LIVE_AUTH_JSON=/path/to/codex/auth.json \
ZHIR_LIVE_INPUT_PACKETS="$PWD/test-results/native-support/input-packets.json" \
  cargo test -p zhir-testing --features openai-live --test gpt_live codex_subscription -- --ignored --nocapture
```

The assertion requires a nonempty provider-observed user transcript distinct from
outbound text. It does not require exact transcription: the service can misrecognize
words, which the printed `spoken_transcripts` evidence preserves.

`codex_subscription_survives_udp_blackout` forwards signaling to the real OAuth
endpoint and rewrites the SDP answer through a test-only UDP relay. It drops both
directions of the nominated ICE path for eight seconds, then checks acknowledged
controls, resumed traffic, one remote creation and completed media/history through
the kernel. This tests continuity of the original peer, not attaching a new peer or
resuming a checkpoint after process restart. Run the two subscription tests serially:

```sh
ZHIR_LIVE_AUTH_JSON=/path/to/codex/auth.json \
  cargo test -p zhir-testing --features openai-live --test gpt_live \
  codex_subscription_ -- --ignored --nocapture --test-threads=1
```

`webrtc_driver` compiles the production internal driver against a deterministic
transport in the testing crate. Its non-Live adapter uses plain control text and
PCMU parameters. Tests hold writes pending to verify control processing, fixed
confirmation expiry, ordered drain/close and diagnostic settlement. They also check
reservation ownership through output mapping and media finalization. These tests
establish driver scheduling, not real network or codec negotiation; `gpt_live` covers
the production Peer with in-process SDP/DataChannel/RTP fixtures. The Live connection
policy is checked separately for a fixed grace and queued-confirmation handling.

Shared confirmation tests reject overlapping or expired confirmations. Live
fixtures also cover missing/mismatched audio-control acknowledgements and closure
before acknowledgement, provider error details delivered before failure, and abnormal
remote closure without a successful close receipt. RTP tests exercise sequence/timestamp wrap, packet loss,
late/duplicate packets and an unexplained source change.

Live scheduling regressions keep the public event port blocked beyond the command
deadline while pause/resume receipts continue, and retain twelve small Opus packets
with an eight-event queue. Separate cases cover actual byte-budget exhaustion,
single-Close settlement, malformed/duplicate protocol events, rejection under event
pressure and preservation of accepted history before failure. Shared media tests retain reservations across packet
mapping, release on consumption, and bound zero-byte markers independently of bytes.

To investigate the remaining subscription boundaries reproducibly:

```sh
uv run crates/zhir-testing/scripts/live_probe.py \
  --input test-results/native-support/input-packets.json \
  --output test-results/native-support/subscription-boundaries
```

Use a fresh output directory, `--auth` for a Codex OAuth JSON file and `--proxy` if
`HTTPS_PROXY` is unset. `--mode close|kill|both` selects explicit peer destruction
or SIGKILL of the separate Rust media process. The runner creates one remote call
per case, verifies sideband detach/reattach and input controls, sends output-control
probes during real speech, attempts sideband access after the media process exits,
and tries the public fork endpoint. The Rust diagnostic uses production WebRTC
transport source; no copied transport or protocol fallback is shipped.

Evidence contains HTTP status, call identity, correlated command errors, received
transcripts, RTP identities, process exit and sideband events. PCM observer payloads
are omitted from this diagnostic. An exit code of zero means the probe completed,
not that interruption or recovery passed. The JSON retains rejections and the model
README describes the observed limitations. This is protocol investigation, distinct
from the native Runtime E2E and the original-peer UDP blackout test above.

To check whether the same OAuth account can start a public primary audio WebSocket:

```sh
uv run crates/zhir-testing/scripts/live_probe.py --mode primary \
  --output test-results/native-support/primary-access
```

This mode needs no speech fixture or Rust media process. It tests startup at
`wss://api.openai.com/v1/live/sessions` with `gpt-live-1-codex` and `gpt-live-1`,
records startup rejection separately from the WebSocket handshake, and closes any
started session. It does not test storage, fork, interruption or media recovery.
Access to an existing-call sideband does not prove that this primary connection
is available to the OAuth account.
