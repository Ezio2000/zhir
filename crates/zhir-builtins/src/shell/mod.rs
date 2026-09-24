use crate::common::{MUTATING, failure, spec};
use serde::Deserialize;
use serde_json::json;
use std::{
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use zhir_core::{Result, tool::RuntimeTool};
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
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct Args {
    #[schemars(length(min = 1))]
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
/// Output still open after the process group stops belongs to escaped descendants.
const DRAIN_GRACE: Duration = Duration::from_millis(500);
#[derive(Default)]
struct Captured {
    kept: Vec<u8>,
    truncated: bool,
}
async fn drain(
    mut input: impl AsyncRead + Unpin,
    limit: usize,
    captured: Arc<Mutex<Captured>>,
) -> std::io::Result<()> {
    let mut buffer = [0u8; 8192];
    loop {
        let n = input.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        let mut captured = captured.lock().expect("shell output");
        let take = n.min(limit.saturating_sub(captured.kept.len()));
        captured.kept.extend_from_slice(&buffer[..take]);
        captured.truncated |= take < n;
    }
}
struct Stream {
    captured: Arc<Mutex<Captured>>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Stream {
    fn spawn(input: impl AsyncRead + Unpin + Send + 'static, limit: usize) -> Self {
        let captured = Arc::<Mutex<Captured>>::default();
        let task = tokio::spawn(drain(input, limit, captured.clone()));
        Self { captured, task }
    }
    /// An unfinished stream at `until` keeps its partial output, marked truncated.
    async fn finish(mut self, until: tokio::time::Instant) -> Result<(String, bool)> {
        let ended = match tokio::time::timeout_at(until, &mut self.task).await {
            Ok(result) => {
                result
                    .map_err(|e| failure("command_output", e))?
                    .map_err(|e| failure("command_output", e))?;
                true
            }
            Err(_) => {
                self.task.abort();
                false
            }
        };
        let captured = self.captured.lock().expect("shell output");
        Ok((
            String::from_utf8_lossy(&captured.kept).into_owned(),
            captured.truncated || !ended,
        ))
    }
}
pub fn bash(options: ShellOptions) -> Result<Arc<dyn RuntimeTool>> {
    if !options.cwd.is_dir() || options.timeout.is_zero() || options.max_output_bytes == 0 {
        return Err(zhir_core::error::Error::Invalid(
            "invalid shell options".into(),
        ));
    }
    Ok(Arc::new(zhir_tools::function::structured(
        spec::<Args>(
            "bash",
            "Execute a shell command in the workspace.",
            MUTATING,
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
                let output = Stream::spawn(stdout, options.max_output_bytes);
                let errors = Stream::spawn(stderr, options.max_output_bytes);
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
                let until = tokio::time::Instant::now() + DRAIN_GRACE;
                let stdout = output.finish(until).await?;
                let stderr = errors.finish(until).await?;
                context.cancellation.check()?;
                let Some(status) = status else {
                    return Err(failure(
                        "command_timeout",
                        format!("command exceeded {} ms", options.timeout.as_millis()),
                    ));
                };
                Ok(zhir_tools::reply::json(
                    json!({"exit_code":status.code(),"stdout":stdout.0,"stderr":stderr.0,"truncated":stdout.1||stderr.1}),
                ))
            }
        },
    )?))
}
