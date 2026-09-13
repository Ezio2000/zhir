# 测试与验证

使用 `rust-toolchain.toml` 指定的工具链和已提交的 `Cargo.lock`。
本页维护可复现的方法；运行日志、统计和原始响应放在被忽略的 `test-results/`、临时目录
或 CI artifacts 中。具体检查清单以 [CI workflow](../.github/workflows/ci.yml) 为准。

## 常规检查

在仓库根目录执行：

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features --locked
cargo run -p zhir-conformance --bin zhir-conformance
cargo run -p zhir-conformance --bin schemas -- --check
```

普通测试使用本地模型和 HTTP/SSE 夹具。真实模型、MySQL 和 Redis 测试标为 ignored，
必须显式提供环境并运行，不会在普通工作区测试中调用外部服务。

| 入口 | 覆盖范围 |
| --- | --- |
| `conformance/tests/dependencies.rs` | 生产依赖边界与子 Agent feature 分离 |
| `conformance/cases/` | 状态、控制、审批、原子提交、历史、错误和限制 |
| `crates/zhir-core/tests/` | 值校验、完整限额序列化、历史结构及显式时钟下的提交校验 |
| `crates/zhir-policies/tests/` | 重试预算、自定义退避、溢出边界和无执行器历史窗口 |
| `crates/zhir/tests/core_boundaries.rs` | 目录选择与绑定、运行创建、显式能力和执行错误呈现 |
| `crates/zhir-tools/tests/` | 工具目录、Schema、类型化结果和执行装饰器 |
| `crates/zhir-models/tests/` | 协议编码、SSE、扩展会话与模型装饰器 |
| `crates/zhir-builtins/tests/` | 文件、Shell、交互和子 Agent |
| `crates/zhir/tests/developer_api.rs` | 参数隔离、暂停票据、恢复冲突和研发 API 组合 |
| `crates/zhir/tests/provider_integration.rs` | 用户能力适配、执行归属、媒体持久化与重放 |
| `crates/zhir/tests/http_fixture.rs` | 传输捕获、分片、延迟、断连与清理 |
| `crates/zhir/tests/consumer_ten.rs` | 目录组合、选择固化、类型上下文、结果、审批、响应变换、重试、匹配脚本与产物存储 |
| `crates/zhir/tests/consumer_six.rs` | 消费者转写能力、类型化复核和恢复闭环 |
| `crates/zhir/tests/scenario_scale/` | 并发、请求组合与密集流式场景 |

修改 wire DTO 后，用 `cargo run -p zhir-conformance --bin schemas` 重新生成
`contracts/v1/schemas/`，并运行一致性检查。Schema 和 conformance fixtures 需要提交。

## Feature、示例与包

独立 feature 检查验证可选依赖边界；完整矩阵由 CI 维护。针对修改涉及的 feature 执行：

```sh
cargo check -p zhir --no-default-features --locked
cargo check -p zhir-core --no-default-features --locked
cargo test -p zhir-policies --locked
cargo test -p zhir-builtins --no-default-features --features agent --locked
cargo test -p zhir-builtins --no-default-features --features agent-runtime --locked
cargo check -p zhir --no-default-features --features policies --locked
cargo check -p zhir --no-default-features --features typed-tools --locked
cargo check -p zhir-testing --no-default-features --locked
cargo check -p zhir-testing --no-default-features --features http --locked
cargo test -p zhir --no-default-features --features models,typed-tools,memory --test developer_api
cargo test -p zhir --no-default-features --features models,typed-tools,memory --test consumer_ten
cargo test -p zhir --no-default-features --features models,typed-tools,memory --test core_boundaries
cargo check -p zhir --no-default-features --features artifacts-filesystem --locked
cargo test -p zhir --no-default-features --features models,typed-tools,typed-output,memory --test convenience
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --example resume --features interaction,memory
cargo bench -p zhir-core --bench history
cargo bench -p zhir-kernel --bench trace
cargo test -p zhir-models --all-features --release --lib stream_append_scale -- --ignored --nocapture
cargo package --workspace --allow-dirty --locked
```

同版本反复打包遇到 Cargo 临时 registry 的旧源码缓存时，用全新的 `--target-dir` 重跑。
包验证应确认生成的 archive 包含当前源码，并成功编译；工作区编译不能代替包验证。
`conformance` 参与工作区验证但不发布。历史基准用于观察增长趋势，不作为性能保证。
history 基准还测量逐个消费 pending 时的追加、校验及状态编码；trace 基准测量递增
checkpoint 历史的离线验证；stream_append_scale 每组使用固定 64 字节片段、取三次中位数。
这些入口报告规模与耗时，不以容易受机器负载影响的时间阈值作为普通测试断言。
普通回归测试验证游标编码大小、跨 chunk 顺序与恢复、重建前缀的校验、原生重放顺序、
流式 Unicode/工具参数/元数据、Schema 约束以及 grep 的提前停止和前后文。

## 真实数据库

启动独立测试数据库，通过环境变量提供连接地址：

```sh
export ZHIR_TEST_MYSQL_URL='mysql://root:password@127.0.0.1:3306/zhir_test'
export ZHIR_TEST_REDIS_URL='redis://127.0.0.1:6379/'
cargo test -p zhir-storage --all-features --test stores -- --ignored
```

CI 使用 MySQL 8.4 和 Redis 7 服务。测试覆盖历史追加与替换、读取恢复、提交幂等、
冲突、截止时间和固化参数校验；Memory 和 SQLite 在普通工作区测试中覆盖。
四种存储都覆盖分批工具游标、暂停恢复和非法游标提交拒绝。
这些检查不等同于分布式故障注入或生产压测。

## 真实模型接入

现有消费者测试使用 `DEEPSEEK_API_KEY`，具体供应商参数和能力映射只存在于测试侧。
Chat 测试直接通过 `ModelOptions.extra` 声明该端点的原生 `max_tokens`；
不设置会生成 `max_completion_tokens` 的通用字段，也不在编码后改名。
这些入口会产生真实模型调用费用，按需要选择一个入口显式运行：

| 测试 target | 测试名称 | 报告路径环境变量 |
| --- | --- | --- |
| `live_protocols` | `live_protocol_matrix` | `DEEPSEEK_LIVE_REPORT` |
| `extensibility` | `live_consumer_capability_audit` | `ZHIR_LIVE_AUDIT_REPORT` |
| `scenario_scale` | `live_scenario_scale` | `ZHIR_SCALE_REPORT` |
| `scenario_scale` | `live_developer_api` | `ZHIR_DEVELOPER_REPORT` |

例如，凭据已在环境中设置后：

```sh
mkdir -p test-results
ZHIR_DEVELOPER_REPORT="$PWD/test-results/developer-live.json" \
  cargo test -p zhir --all-features --test scenario_scale live_developer_api -- --ignored --nocapture
```

报告路径使用绝对路径，避免 Cargo 的包工作目录改变输出位置。基础协议矩阵还支持
`DEEPSEEK_MODEL` 和 `DEEPSEEK_LIVE_FILTER`；规模测试支持 `ZHIR_SCALE_FILTER`、
`ZHIR_SCALE_REPEATS`、`ZHIR_SCALE_CONCURRENCY`，具体值见对应测试源码。
规模报告保留每次模型调用的规范化输出和 finish_reason，用于定位真实响应与预期不符的情况。
子 Agent 用例分别检查结算状态与回执内容，内容不符时报告子运行 ID、期望值和实际值。
复现此类失败应固定原始回执与提示词，并核对服务原始响应；重新生成随机回执不能证明原异常消失。

本地 HTTP/SSE 测试验证接入机制，不证明外部能力服务的行为或媒体质量。
HttpFixture 支持固定长度 HTTP/1.1 请求，不覆盖 TLS、WebSocket 或 chunked 请求。
产物持久化、回收和外部副作用幂等由使用方负责；流式观察不等同于已提交结果。

本地 Agent 回归测试覆盖并发启动满载拒绝、重复 key、取消与完成后的名额释放；
agent 单独 feature 的测试使用外部 AgentBackend。模型集成测试区分未绑定媒体的协议失败
与底层存储故障，断言 checkpoint 状态、revision 及未提交部分输出。所有存储共享的测试
还拒绝错误 parent、不匹配的 delta、空追加及重复 initial，检查原 head 不变。
