use crate::common::{failure, spec};
use serde::Deserialize;
use serde_json::json;
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt};
use zhir_core::{
    Result,
    tool::{RuntimeTool, RuntimeToolResult},
};
#[derive(Clone)]
pub struct ShellOptions {
    pub cwd: PathBuf,
    pub program: String,
    pub timeout: Duration,
    pub max_output_bytes: usize,
}
impl ShellOptions {
    pub fn new(cwd: impl Into<PathBuf>) -> Self {
        Self {
            cwd: cwd.into(),
            program: "bash".into(),
            timeout: Duration::from_secs(120),
            max_output_bytes: 64 * 1024,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    command: String,
}
struct ProcessGroup {
    id: Option<u32>,
}
impl ProcessGroup {
    fn stop(&self) {
        #[cfg(unix)]
        if let Some(id) = self.id {
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(id as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.stop();
    }
}
async fn drain(mut input: impl AsyncRead + Unpin, limit: usize) -> std::io::Result<(String, bool)> {
    let mut kept = Vec::new();
    let mut truncated = false;
    let mut buffer = [0u8; 8192];
    loop {
        let n = input.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        let take = n.min(limit.saturating_sub(kept.len()));
        kept.extend_from_slice(&buffer[..take]);
        truncated |= take < n;
    }
    Ok((String::from_utf8_lossy(&kept).into_owned(), truncated))
}
pub fn bash(options: ShellOptions) -> Result<Arc<dyn RuntimeTool>> {
    if !options.cwd.is_dir() || options.timeout.is_zero() || options.max_output_bytes == 0 {
        return Err(zhir_core::error::Error::Invalid(
            "invalid shell options".into(),
        ));
    }
    Ok(Arc::new(zhir_tools::function::structured(
        spec(
            "bash",
            "Execute a shell command in the workspace.",
            json!({"type":"object","required":["command"],"properties":{"command":{"type":"string","minLength":1}},"additionalProperties":false}),
            false,
        ),
        move |a: Args, context| {
            let options = options.clone();
            async move {
                context.cancellation.check()?;
                let mut command = tokio::process::Command::new(&options.program);
                command
                    .arg("-c")
                    .arg(&a.command)
                    .current_dir(&options.cwd)
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .kill_on_drop(true);
                #[cfg(unix)]
                command.process_group(0);
                let mut child = command.spawn().map_err(|e| failure("command_start", e))?;
                let group = ProcessGroup { id: child.id() };
                let stdout = child.stdout.take().expect("piped stdout");
                let stderr = child.stderr.take().expect("piped stderr");
                let output = tokio::spawn(drain(stdout, options.max_output_bytes));
                let errors = tokio::spawn(drain(stderr, options.max_output_bytes));
                let cancelled = async {
                    while !context.cancellation.is_cancelled() {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                };
                let status = tokio::select! {result=child.wait()=>Some(result.map_err(|e|failure("command_wait",e))?),_=tokio::time::sleep(options.timeout)=>None,_=cancelled=>None};
                group.stop();
                if status.is_none() {
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                }
                let stdout = output
                    .await
                    .map_err(|e| failure("command_output", e))?
                    .map_err(|e| failure("command_output", e))?;
                let stderr = errors
                    .await
                    .map_err(|e| failure("command_output", e))?
                    .map_err(|e| failure("command_output", e))?;
                context.cancellation.check()?;
                let Some(status) = status else {
                    return Err(failure(
                        "command_timeout",
                        format!("command exceeded {} ms", options.timeout.as_millis()),
                    ));
                };
                Ok(RuntimeToolResult::json(
                    json!({"exit_code":status.code(),"stdout":stdout.0,"stderr":stderr.0,"truncated":stdout.1||stderr.1}),
                ))
            }
        },
    )?))
}
