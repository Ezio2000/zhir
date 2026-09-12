use crate::common::{failure, spec};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use zhir_core::{
    Result,
    error::Error,
    tool::{RuntimeTool, RuntimeToolContext, RuntimeToolResult},
};
use zhir_tools::function;
const MAX_BYTES: u64 = 4 * 1024 * 1024;
#[derive(Clone)]
struct Workspace {
    root: PathBuf,
}
impl Workspace {
    fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root
            .into()
            .canonicalize()
            .map_err(|e| failure("filesystem", e))?;
        if !root.is_dir() {
            return Err(Error::Invalid("workspace must be a directory".into()));
        }
        Ok(Self { root })
    }
    fn path(&self, path: &str, create: bool) -> Result<PathBuf> {
        let path = self.root.join(path);
        let resolved = if create && !path.exists() {
            let parent = path
                .parent()
                .ok_or_else(|| Error::Invalid("missing parent".into()))?
                .canonicalize()
                .map_err(|e| failure("filesystem", e))?;
            parent.join(
                path.file_name()
                    .ok_or_else(|| Error::Invalid("missing filename".into()))?,
            )
        } else {
            path.canonicalize().map_err(|e| failure("filesystem", e))?
        };
        if !resolved.starts_with(&self.root) {
            return Err(Error::Invalid("path is outside workspace".into()));
        }
        Ok(resolved)
    }
    fn display(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }
}
fn read(path: &Path) -> Result<Vec<u8>> {
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
fn utf8(bytes: &[u8]) -> Result<&str> {
    if bytes.contains(&0) {
        return Err(failure("binary_file", "binary file"));
    }
    std::str::from_utf8(bytes)
        .map(|s| s.strip_prefix('\u{feff}').unwrap_or(s))
        .map_err(|e| failure("invalid_utf8", e))
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
async fn blocking<F>(context: RuntimeToolContext, operation: F) -> Result<RuntimeToolResult>
where
    F: FnOnce(RuntimeToolContext) -> Result<Value> + Send + 'static,
{
    context.cancellation.check()?;
    tokio::task::spawn_blocking(move || {
        context.cancellation.check()?;
        operation(context).map(RuntimeToolResult::json)
    })
    .await
    .map_err(|e| failure("filesystem", e))?
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    path: String,
    #[serde(default = "one")]
    offset: usize,
    #[serde(default = "two_hundred")]
    limit: usize,
}
fn one() -> usize {
    1
}
fn two_hundred() -> usize {
    200
}
fn thousand() -> usize {
    1000
}
pub fn read_file(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    let tool = function::structured(
        spec(
            "read_file",
            "Read UTF-8 lines and the file digest.",
            json!({"type":"object","required":["path"],"properties":{"path":{"type":"string"},"offset":{"type":"integer","minimum":1},"limit":{"type":"integer","minimum":1,"maximum":2000}},"additionalProperties":false}),
            true,
        ),
        move |a: ReadArgs, context| {
            let w = workspace.clone();
            async move {
                blocking(context,move |_| {if a.offset==0 || a.limit==0 || a.limit>2000 {return Err(Error::Invalid("invalid line range".into()));}let path=w.path(&a.path,false)?;let bytes=read(&path)?;let text=utf8(&bytes)?;let lines=text.lines().collect::<Vec<_>>();let selected=lines.iter().skip(a.offset-1).take(a.limit).copied().collect::<Vec<_>>().join("\n");Ok(json!({"path":w.display(&path),"content":selected,"offset":a.offset,"total_lines":lines.len(),"truncated":lines.len()>a.offset-1+a.limit,"sha256":digest(&bytes)}))}).await
            }
        },
    )?;
    Ok(Arc::new(tool))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    #[serde(default = "dot")]
    path: String,
    #[serde(default = "thousand")]
    limit: usize,
}
fn dot() -> String {
    ".".into()
}
pub fn list_files(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec(
            "list_files",
            "List directory entries.",
            json!({"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10000}},"additionalProperties":false}),
            true,
        ),
        move |a: ListArgs, context| {
            let w = workspace.clone();
            async move {
                blocking(context,move |ctx| {let root=w.path(&a.path,false)?;let mut entries=std::fs::read_dir(root).map_err(|e|failure("filesystem",e))?.collect::<std::io::Result<Vec<_>>>().map_err(|e|failure("filesystem",e))?;entries.sort_by_key(|e|e.file_name());let truncated=entries.len()>a.limit;let mut output=Vec::new();for entry in entries.into_iter().take(a.limit) {ctx.cancellation.check()?;let ty=entry.file_type().map_err(|e|failure("filesystem",e))?;output.push(json!({"path":w.display(&entry.path()),"kind":if ty.is_dir() {"directory"} else if ty.is_symlink() {"symlink"} else {"file"}}));}Ok(json!({"entries":output,"truncated":truncated}))}).await
            }
        },
    )?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobArgs {
    pattern: String,
    #[serde(default = "dot")]
    path: String,
    #[serde(default = "thousand")]
    limit: usize,
    #[serde(default)]
    include_hidden: bool,
}
pub fn glob(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec(
            "glob",
            "Find workspace paths by glob.",
            json!({"type":"object","required":["pattern"],"properties":{"pattern":{"type":"string"},"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10000},"include_hidden":{"type":"boolean"}},"additionalProperties":false}),
            true,
        ),
        move |a: GlobArgs, context| {
            let w = workspace.clone();
            async move {
                blocking(context, move |ctx| {
                    let root = w.path(&a.path, false)?;
                    let pattern = globset::Glob::new(&a.pattern)
                        .map_err(|e| Error::Invalid(e.to_string()))?
                        .compile_matcher();
                    let mut found = Vec::new();
                    for entry in walkdir::WalkDir::new(&root)
                        .sort_by_file_name()
                        .into_iter()
                        .filter_entry(|e| {
                            a.include_hidden
                                || e.depth() == 0
                                || !e.file_name().to_string_lossy().starts_with('.')
                        })
                    {
                        ctx.cancellation.check()?;
                        let entry = entry.map_err(|e| failure("filesystem", e))?;
                        if entry.depth() == 0 {
                            continue;
                        }
                        let relative = entry.path().strip_prefix(&root).expect("walk root");
                        if pattern.is_match(relative) {
                            found.push(w.display(entry.path()));
                            if found.len() > a.limit {
                                break;
                            }
                        }
                    }
                    let truncated = found.len() > a.limit;
                    found.truncate(a.limit);
                    Ok(json!({"matches":found,"truncated":truncated}))
                })
                .await
            }
        },
    )?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrepArgs {
    pattern: String,
    #[serde(default = "dot")]
    path: String,
    #[serde(default = "two_hundred")]
    limit: usize,
    #[serde(default)]
    ignore_case: bool,
    #[serde(default)]
    literal: bool,
    #[serde(default)]
    context: usize,
    #[serde(default = "files_mode")]
    output_mode: String,
    glob: Option<String>,
}
fn files_mode() -> String {
    "files_with_matches".into()
}
pub fn grep(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec(
            "grep",
            "Search UTF-8 files, returning content, paths or counts.",
            json!({"type":"object","required":["pattern"],"properties":{"pattern":{"type":"string"},"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":10000},"ignore_case":{"type":"boolean"},"literal":{"type":"boolean"},"context":{"type":"integer","minimum":0,"maximum":100},"output_mode":{"enum":["content","files_with_matches","count"]},"glob":{"type":"string"}},"additionalProperties":false}),
            true,
        ),
        move |a: GrepArgs, context| {
            let w = workspace.clone();
            async move {
                blocking(context, move |ctx| {
                    let root = w.path(&a.path, false)?;
                    let pattern = if a.literal {
                        fancy_regex::escape(&a.pattern).into_owned()
                    } else {
                        a.pattern.clone()
                    };
                    let pattern = if a.ignore_case { format!("(?i){pattern}") } else { pattern };
                    let regex = fancy_regex::RegexBuilder::new(&pattern)
                        .backtrack_limit(100_000).build()
                        .map_err(|e| Error::Invalid(e.to_string()))?;
                    let glob = a.glob.map(|s| globset::Glob::new(&s)
                        .map(|g| g.compile_matcher())
                        .map_err(|e| Error::Invalid(e.to_string()))).transpose()?;
                    let mut results = Vec::new();
                    let mut searched = 0;
                    let mut skipped = 0;
                    let mut truncated = false;
                    'files: for entry in walkdir::WalkDir::new(&root).sort_by_file_name() {
                        ctx.cancellation.check()?;
                        let entry = entry.map_err(|e| failure("filesystem", e))?;
                        if !entry.file_type().is_file() { continue; }
                        if glob.as_ref().is_some_and(|g| !g.is_match(entry.path().strip_prefix(&w.root).unwrap_or(entry.path()))) {
                            continue;
                        }
                        let bytes = match read(entry.path()) {
                            Ok(bytes) => bytes,
                            Err(_) => { skipped += 1; continue; }
                        };
                        let text = match utf8(&bytes) {
                            Ok(text) => text,
                            Err(_) => { skipped += 1; continue; }
                        };
                        searched += 1;
                        let lines = text.lines().collect::<Vec<_>>();
                        let mut matches = Vec::new();
                        for (index, line) in lines.iter().enumerate() {
                            ctx.cancellation.check()?;
                            if regex.is_match(line).map_err(|e| failure("regex_limit", e))? {
                                matches.push(index);
                            }
                        }
                        if matches.is_empty() { continue; }
                        let path = w.display(entry.path());
                        match a.output_mode.as_str() {
                            "files_with_matches" => results.push(json!(path)),
                            "count" => results.push(json!({"path":path,"count":matches.len()})),
                            "content" => {
                                for i in matches {
                                    results.push(json!({"path":path,"line":i+1,"text":lines[i],"before":lines[i.saturating_sub(a.context)..i],"after":lines[i+1..(i+1+a.context).min(lines.len())]}));
                                    if results.len() > a.limit {
                                        truncated = true;
                                        break 'files;
                                    }
                                }
                            }
                            _ => return Err(Error::Invalid("unknown output mode".into())),
                        }
                        if results.len() > a.limit {
                            truncated = true;
                            break;
                        }
                    }
                    results.truncate(a.limit);
                    Ok(json!({"output_mode":a.output_mode,"results":results,"truncated":truncated,"files_searched":searched,"files_skipped":skipped}))
                }).await
            }
        },
    )?))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    path: String,
    content: String,
    expected_sha256: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EditArgs {
    path: String,
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
        spec(
            "write_file",
            "Create or replace a UTF-8 file using its expected digest.",
            json!({"type":"object","required":["path","content","expected_sha256"],"properties":{"path":{"type":"string"},"content":{"type":"string"},"expected_sha256":{"type":["string","null"]}},"additionalProperties":false}),
            false,
        ),
        move |a: WriteArgs, context| {
            let w = workspace.clone();
            async move {
                blocking(context,move |ctx| {let path=w.path(&a.path,true)?;let previous=replace_file(&path,a.content.as_bytes(),a.expected_sha256.as_deref(),&ctx)?;Ok(json!({"path":w.display(&path),"operation":if previous.is_some() {"replace"} else {"create"},"previous_sha256":previous,"sha256":digest(a.content.as_bytes()),"bytes_written":a.content.len()}))}).await
            }
        },
    )?))
}
pub fn edit_file(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec(
            "edit_file",
            "Replace matching text using the current file digest.",
            json!({"type":"object","required":["path","old_string","new_string","expected_sha256"],"properties":{"path":{"type":"string"},"old_string":{"type":"string","minLength":1},"new_string":{"type":"string"},"expected_sha256":{"type":"string"},"replace_all":{"type":"boolean"}},"additionalProperties":false}),
            false,
        ),
        move |a: EditArgs, context| {
            let w = workspace.clone();
            async move {
                blocking(context,move |ctx| {let path=w.path(&a.path,false)?;let bytes=read(&path)?;let text=utf8(&bytes)?;if a.old_string.is_empty() {return Err(Error::Invalid("empty match text".into()));}let count=text.matches(&a.old_string).count();if count==0 || (count>1 && !a.replace_all) {return Err(failure("ambiguous_edit","expected one match, or replace_all"));}let content=if a.replace_all {text.replace(&a.old_string,&a.new_string)} else {text.replacen(&a.old_string,&a.new_string,1)};replace_file(&path,content.as_bytes(),Some(&a.expected_sha256),&ctx)?;Ok(json!({"path":w.display(&path),"replacements":count,"previous_sha256":a.expected_sha256,"sha256":digest(content.as_bytes()),"bytes_written":content.len()}))}).await
            }
        },
    )?))
}
pub fn tools(root: impl Into<PathBuf>) -> Result<Vec<Arc<dyn RuntimeTool>>> {
    let root = root.into();
    Ok(vec![
        read_file(&root)?,
        list_files(&root)?,
        glob(&root)?,
        grep(&root)?,
        write_file(&root)?,
        edit_file(&root)?,
    ])
}
