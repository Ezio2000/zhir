use super::workspace::*;
use crate::common::{failure, spec};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use zhir_core::{
    Result,
    error::Error,
    tool::{RuntimeTool, RuntimeToolContext},
};
use zhir_tools::function;
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    path: String,
    #[serde(default = "one")]
    #[schemars(range(min = 1))]
    offset: usize,
    #[serde(default = "two_hundred")]
    #[schemars(range(min = 1, max = MAX_READ_LINES))]
    limit: usize,
}
pub fn read_file(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    let tool = function::structured(
        spec::<ReadArgs>("read_file", "Read UTF-8 lines and the file digest.", true),
        move |a: ReadArgs, context| {
            let w = workspace.clone();
            async move { blocking(context, move |_| read_content(&w, a)).await }
        },
    )?;
    Ok(Arc::new(tool))
}
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    #[serde(default = "dot")]
    path: String,
    #[serde(default = "thousand")]
    #[schemars(range(min = 1, max = 10000))]
    limit: usize,
}
pub fn list_files(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec::<ListArgs>("list_files", "List directory entries.", true),
        move |a: ListArgs, context| {
            let w = workspace.clone();
            async move { blocking(context, move |ctx| list_content(&w, a, &ctx)).await }
        },
    )?))
}
fn read_content(w: &Workspace, a: ReadArgs) -> Result<Value> {
    if a.offset == 0 || a.limit == 0 || a.limit > MAX_READ_LINES {
        return Err(Error::Invalid("invalid line range".into()));
    }
    let path = w.path(&a.path, false)?;
    let bytes = read(&path)?;
    let text = utf8(&bytes)?;
    let lines = text.lines();
    let total_lines = lines.clone().count();
    let selected = lines
        .skip(a.offset - 1)
        .take(a.limit)
        .collect::<Vec<_>>()
        .join("\n");
    Ok(
        json!({"path":w.display(&path),"content":selected,"offset":a.offset,"total_lines":total_lines,"truncated":total_lines.saturating_sub(a.offset-1)>a.limit,"sha256":digest(&bytes)}),
    )
}

fn list_content(w: &Workspace, a: ListArgs, ctx: &RuntimeToolContext) -> Result<Value> {
    let root = w.path(&a.path, false)?;
    let mut entries = std::fs::read_dir(root)
        .map_err(|e| failure("filesystem", e))?
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|e| failure("filesystem", e))?;
    entries.sort_by_key(|e| e.file_name());
    let truncated = entries.len() > a.limit;
    let mut output = Vec::new();
    for entry in entries.into_iter().take(a.limit) {
        ctx.cancellation.check()?;
        let ty = entry.file_type().map_err(|e| failure("filesystem", e))?;
        output.push(json!({"path":w.display(&entry.path()),"kind":if ty.is_dir() {"directory"} else if ty.is_symlink() {"symlink"} else {"file"}}));
    }
    Ok(json!({"entries":output,"truncated":truncated}))
}
