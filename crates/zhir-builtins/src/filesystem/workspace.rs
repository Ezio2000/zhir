use crate::common::failure;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    io::Read,
    path::{Path, PathBuf},
};
use zhir_core::{Result, error::Error, operation::ToolExecution, tool::RuntimeToolContext};
pub(super) const MAX_BYTES: u64 = 4 * 1024 * 1024;
pub(super) const MAX_READ_LINES: usize = 2000;
#[derive(Clone)]
pub(super) struct Workspace {
    pub(super) root: PathBuf,
}
impl Workspace {
    pub(super) fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root
            .into()
            .canonicalize()
            .map_err(|e| failure("filesystem", e))?;
        if !root.is_dir() {
            return Err(Error::Invalid("workspace must be a directory".into()));
        }
        Ok(Self { root })
    }
    pub(super) fn path(&self, path: &str, create: bool) -> Result<PathBuf> {
        let path = self.root.join(path);
        let resolved = if create && !path.exists() {
            let parent = path
                .parent()
                .ok_or_else(|| failure("invalid_path", "missing parent"))?
                .canonicalize()
                .map_err(|e| failure("filesystem", e))?;
            parent.join(
                path.file_name()
                    .ok_or_else(|| failure("invalid_path", "missing filename"))?,
            )
        } else {
            path.canonicalize().map_err(|e| failure("filesystem", e))?
        };
        if !resolved.starts_with(&self.root) {
            return Err(failure("outside_workspace", "path is outside workspace"));
        }
        Ok(resolved)
    }
    pub(super) fn display(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }
}
pub(super) fn read(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|e| failure("filesystem", e))?
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| failure("filesystem", e))?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(failure("file_too_large", "file exceeds 4 MiB"));
    }
    Ok(bytes)
}
pub(super) fn utf8(bytes: &[u8]) -> Result<&str> {
    if bytes.contains(&0) {
        return Err(failure("binary_file", "binary file"));
    }
    std::str::from_utf8(bytes)
        .map(|s| s.strip_prefix('\u{feff}').unwrap_or(s))
        .map_err(|e| failure("invalid_utf8", e))
}
pub(super) fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) async fn blocking<F>(context: RuntimeToolContext, operation: F) -> Result<ToolExecution>
where
    F: FnOnce(RuntimeToolContext) -> Result<Value> + Send + 'static,
{
    context.cancellation.check()?;
    tokio::task::spawn_blocking(move || {
        context.cancellation.check()?;
        operation(context).map(zhir_tools::reply::json)
    })
    .await
    .map_err(|e| failure("filesystem", e))?
}
pub(super) fn one() -> usize {
    1
}
pub(super) fn two_hundred() -> usize {
    200
}
pub(super) fn thousand() -> usize {
    1000
}
pub(super) fn dot() -> String {
    ".".into()
}
