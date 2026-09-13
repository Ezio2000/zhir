use super::workspace::*;
use crate::common::{failure, spec};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io::Write, path::Path};
use std::{path::PathBuf, sync::Arc};
use zhir_core::{
    Result,
    error::Error,
    tool::{RuntimeTool, RuntimeToolContext},
};
use zhir_tools::function;
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
enum ExpectedDigest {
    Existing(String),
    Absent,
}
impl ExpectedDigest {
    fn as_deref(&self) -> Option<&str> {
        match self {
            Self::Existing(value) => Some(value),
            Self::Absent => None,
        }
    }
}
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    path: String,
    content: String,
    expected_sha256: ExpectedDigest,
}
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct EditArgs {
    path: String,
    #[schemars(length(min = 1))]
    old_string: String,
    new_string: String,
    expected_sha256: String,
    #[serde(default)]
    replace_all: bool,
}
fn replace_file(
    path: &Path,
    content: &[u8],
    expected: Option<&str>,
    ctx: &RuntimeToolContext,
) -> Result<Option<String>> {
    if content.len() as u64 > MAX_BYTES {
        return Err(failure("file_too_large", "new content exceeds 4 MiB"));
    }
    let previous = if path.exists() {
        Some(digest(&read(path)?))
    } else {
        None
    };
    if previous.as_deref() != expected {
        return Err(failure(
            "file_changed",
            "file digest differs from expected value",
        ));
    }
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().expect("resolved parent"))
        .map_err(|e| failure("filesystem", e))?;
    if let Ok(metadata) = std::fs::metadata(path) {
        temp.as_file()
            .set_permissions(metadata.permissions())
            .map_err(|e| failure("filesystem", e))?;
    }
    temp.write_all(content)
        .map_err(|e| failure("filesystem", e))?;
    temp.as_file()
        .sync_all()
        .map_err(|e| failure("filesystem", e))?;
    ctx.cancellation.check()?;
    if expected.is_none() {
        temp.persist_noclobber(path)
            .map_err(|e| failure("filesystem", e))?;
    } else {
        let latest = digest(&read(path)?);
        if Some(latest.as_str()) != expected {
            return Err(failure("file_changed", "file changed during write"));
        }
        temp.persist(path).map_err(|e| failure("filesystem", e))?;
    }
    Ok(previous)
}
pub fn write_file(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec::<WriteArgs>(
            "write_file",
            "Create or replace a UTF-8 file using its expected digest.",
            false,
        ),
        move |a: WriteArgs, context| {
            let w = workspace.clone();
            async move { blocking(context, move |ctx| write_content(&w, a, &ctx)).await }
        },
    )?))
}
pub fn edit_file(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec::<EditArgs>(
            "edit_file",
            "Replace matching text using the current file digest.",
            false,
        ),
        move |a: EditArgs, context| {
            let w = workspace.clone();
            async move { blocking(context, move |ctx| edit_content(&w, a, &ctx)).await }
        },
    )?))
}
fn write_content(w: &Workspace, a: WriteArgs, ctx: &RuntimeToolContext) -> Result<Value> {
    let path = w.path(&a.path, true)?;
    let previous = replace_file(
        &path,
        a.content.as_bytes(),
        a.expected_sha256.as_deref(),
        ctx,
    )?;
    Ok(
        json!({"path":w.display(&path),"operation":if previous.is_some() {"replace"} else {"create"},"previous_sha256":previous,"sha256":digest(a.content.as_bytes()),"bytes_written":a.content.len()}),
    )
}

fn edit_content(w: &Workspace, a: EditArgs, ctx: &RuntimeToolContext) -> Result<Value> {
    let path = w.path(&a.path, false)?;
    let bytes = read(&path)?;
    let text = utf8(&bytes)?;
    if a.old_string.is_empty() {
        return Err(Error::Invalid("empty match text".into()));
    }
    let count = text.matches(&a.old_string).count();
    if count == 0 || (count > 1 && !a.replace_all) {
        return Err(failure(
            "ambiguous_edit",
            "expected one match, or replace_all",
        ));
    }
    let content = if a.replace_all {
        text.replace(&a.old_string, &a.new_string)
    } else {
        text.replacen(&a.old_string, &a.new_string, 1)
    };
    replace_file(&path, content.as_bytes(), Some(&a.expected_sha256), ctx)?;
    Ok(
        json!({"path":w.display(&path),"replacements":count,"previous_sha256":a.expected_sha256,"sha256":digest(content.as_bytes()),"bytes_written":content.len()}),
    )
}
