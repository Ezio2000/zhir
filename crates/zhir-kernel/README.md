# zhir-kernel

Owns Runtime, Invocation, controls, bounded tool scheduling and checkpoint commits. Supply models, catalogs and stores through zhir-core traits. The host supplies a Tokio runtime.

Part of the zhir Cargo workspace, version 0.1.0.

RunRequest, ResumeRequest, ResumeTarget and SuspensionSelector belong to kernel,
re-exported by the SDK. defaults::pause() supplies the host pause preset.
Planning, checkpoint commits, effect interruption and individual tool calls are
private modules over the same Engine. Defaults have separate catalog, batch and
ephemeral-store modules.
`defaults::context()` creates a fresh UUID and captures the start time;
`defaults::limits()` and `defaults::run_options()` provide explicit runtime presets.
Supplying RunContext preserves caller identity, time and metadata.

RuntimeBuilder assembles resources and default RunOptions. start(RunRequest)
resolves whole-field overrides and commits effective options at revision zero.
continue_from and async resume(ResumeRequest) always use checkpoint options.
SuspensionTicket resumption reads the configured store and checks the exact
run/checkpoint/revision/suspension before entering the single commit path.

Invocation::result returns RunCompletion; checkpoint access is explicit. Catalogs
receive run context and cancellation; kernel wraps the frozen selection for both
declaration and binding. Kernel also maps structured errors into execution failures.
