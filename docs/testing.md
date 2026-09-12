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
| `conformance/cases/` | 状态、控制、审批、原子提交、历史、错误和限制 |
| `crates/zhir-core/tests/` | 值校验、序列化和历史结构 |
| `crates/zhir-tools/tests/` | 工具目录、Schema、类型化结果和执行装饰器 |
| `crates/zhir-models/tests/` | 协议编码、SSE、扩展会话与模型装饰器 |
| `crates/zhir-builtins/tests/` | 文件、Shell、交互和子 Agent |
| `crates/zhir/tests/developer_api.rs` | 参数隔离、暂停票据、恢复冲突和研发 API 组合 |
| `crates/zhir/tests/provider_integration.rs` | 用户能力适配、执行归属、媒体持久化与重放 |
| `crates/zhir/tests/http_fixture.rs` | 传输捕获、分片、延迟、断连与清理 |
| `crates/zhir/tests/consumer_six.rs` | 消费者转写能力、类型化复核和恢复闭环 |
| `crates/zhir/tests/scenario_scale/` | 并发、请求组合与密集流式场景 |

修改 wire DTO 后，用 `cargo run -p zhir-conformance --bin schemas` 重新生成
`contracts/v1/schemas/`，并运行一致性检查。Schema 和 conformance fixtures 需要提交。

## Feature、示例与包

独立 feature 检查验证可选依赖边界；完整矩阵由 CI 维护。针对修改涉及的 feature 执行：

```sh
cargo check -p zhir --no-default-features --locked
cargo check -p zhir --no-default-features --features typed-tools --locked
cargo check -p zhir-testing --no-default-features --locked
cargo check -p zhir-testing --no-default-features --features http --locked
cargo test -p zhir --no-default-features --features models,typed-tools,memory --test developer_api
cargo test -p zhir --no-default-features --features models,typed-tools,typed-output,memory --test convenience
cargo run -p zhir --no-default-features --example custom_tool --features models,typed-tools
cargo run -p zhir --example resume --features interaction,memory
cargo bench -p zhir-core --bench history
cargo package --workspace --allow-dirty --locked
```

同版本反复打包遇到 Cargo 临时 registry 的旧源码缓存时，用全新的 `--target-dir` 重跑。
包验证应确认生成的 archive 包含当前源码，并成功编译；工作区编译不能代替包验证。
`conformance` 参与工作区验证但不发布。历史基准用于观察增长趋势，不作为性能保证。

## 真实数据库

启动独立测试数据库，通过环境变量提供连接地址：

```sh
export ZHIR_TEST_MYSQL_URL='mysql://root:password@127.0.0.1:3306/zhir_test'
export ZHIR_TEST_REDIS_URL='redis://127.0.0.1:6379/'
cargo test -p zhir-storage --all-features --test stores -- --ignored
```

CI 使用 MySQL 8.4 和 Redis 7 服务。测试覆盖历史追加与替换、读取恢复、提交幂等、
冲突、截止时间和固化参数校验；Memory 和 SQLite 在普通工作区测试中覆盖。
这些检查不等同于分布式故障注入或生产压测。

## 真实模型接入

现有消费者测试使用 `DEEPSEEK_API_KEY`，具体供应商参数和能力映射只存在于测试侧。
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

本地 HTTP/SSE 测试验证接入机制，不证明外部能力服务的行为或媒体质量。
HttpFixture 支持固定长度 HTTP/1.1 请求，不覆盖 TLS、WebSocket 或 chunked 请求。
产物持久化、回收和外部副作用幂等由使用方负责；流式观察不等同于已提交结果。
