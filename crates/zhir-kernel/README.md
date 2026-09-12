# zhir-kernel

Owns Runtime, Invocation, controls, bounded tool scheduling and checkpoint commits. Supply models, catalogs and stores through zhir-core traits. The host supplies a Tokio runtime.

Part of the zhir Cargo workspace, version 0.1.0.

RuntimeBuilder assembles resources and default RunOptions. start(RunRequest)
resolves whole-field overrides and commits effective options at revision zero.
continue_from and async resume(ResumeRequest) always use checkpoint options.
SuspensionTicket resumption reads the configured store and checks the exact
run/checkpoint/revision/suspension before entering the single commit path.
