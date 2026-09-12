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
const MAX_READ_LINES: usize = 2000;
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
fn dot() -> String {
    ".".into()
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
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct GlobArgs {
    pattern: String,
    #[serde(default = "dot")]
    path: String,
    #[serde(default = "thousand")]
    #[schemars(range(min = 1, max = 10000))]
    limit: usize,
    #[serde(default)]
    include_hidden: bool,
}
pub fn glob(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec::<GlobArgs>("glob", "Find workspace paths by glob.", true),
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
#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct GrepArgs {
    pattern: String,
    #[serde(default = "dot")]
    path: String,
    #[serde(default = "two_hundred")]
    #[schemars(range(min = 1, max = 10000))]
    limit: usize,
    #[serde(default)]
    ignore_case: bool,
    #[serde(default)]
    literal: bool,
    #[serde(default)]
    #[schemars(range(max = 100))]
    context: usize,
    #[serde(default)]
    output_mode: GrepOutputMode,
    glob: Option<String>,
}
#[derive(Debug, Clone, Copy, Default, Deserialize, serde::Serialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
enum GrepOutputMode {
    Content,
    #[default]
    FilesWithMatches,
    Count,
}
pub fn grep(root: impl Into<PathBuf>) -> Result<Arc<dyn RuntimeTool>> {
    let workspace = Workspace::new(root)?;
    Ok(Arc::new(function::structured(
        spec::<GrepArgs>(
            "grep",
            "Search UTF-8 files, returning content, paths or counts.",
            true,
        ),
        move |args: GrepArgs, context| {
            let workspace = workspace.clone();
            async move {
                blocking(context, move |ctx| {
                    GrepSearch::new(workspace, args)?.run(&ctx)
                })
                .await
            }
        },
    )?))
}
struct GrepSearch {
    workspace: Workspace,
    args: GrepArgs,
    regex: fancy_regex::Regex,
    glob: Option<globset::GlobMatcher>,
}
impl GrepSearch {
    fn new(workspace: Workspace, args: GrepArgs) -> Result<Self> {
        let pattern = if args.literal {
            fancy_regex::escape(&args.pattern).into_owned()
        } else {
            args.pattern.clone()
        };
        let pattern = if args.ignore_case {
            format!("(?i){pattern}")
        } else {
            pattern
        };
        let regex = fancy_regex::RegexBuilder::new(&pattern)
            .backtrack_limit(100_000)
            .build()
            .map_err(|e| Error::Invalid(e.to_string()))?;
        let glob = args
            .glob
            .as_ref()
            .map(|s| {
                globset::Glob::new(s)
                    .map(|g| g.compile_matcher())
                    .map_err(|e| Error::Invalid(e.to_string()))
            })
            .transpose()?;
        Ok(Self {
            workspace,
            args,
            regex,
            glob,
        })
    }
    fn run(self, ctx: &RuntimeToolContext) -> Result<Value> {
        let root = self.workspace.path(&self.args.path, false)?;
        let mut results = Vec::new();
        let mut searched = 0;
        let mut skipped = 0;
        let mut truncated = false;
        for entry in walkdir::WalkDir::new(root).sort_by_file_name() {
            ctx.cancellation.check()?;
            let entry = entry.map_err(|e| failure("filesystem", e))?;
            if !entry.file_type().is_file() || !self.accepts(entry.path()) {
                continue;
            }
            let Ok(bytes) = read(entry.path()) else {
                skipped += 1;
                continue;
            };
            let Ok(text) = utf8(&bytes) else {
                skipped += 1;
                continue;
            };
            searched += 1;
            let path = self.workspace.display(entry.path());
            if self.scan_file(text, &path, &mut results, ctx)? {
                truncated = true;
                break;
            }
        }
        Ok(
            json!({"output_mode":self.args.output_mode,"results":results,"truncated":truncated,
            "files_searched":searched,"files_skipped":skipped}),
        )
    }
    fn accepts(&self, path: &Path) -> bool {
        self.glob
            .as_ref()
            .is_none_or(|g| g.is_match(path.strip_prefix(&self.workspace.root).unwrap_or(path)))
    }
    fn matches(&self, line: &str, ctx: &RuntimeToolContext) -> Result<bool> {
        ctx.cancellation.check()?;
        self.regex
            .is_match(line)
            .map_err(|e| failure("regex_limit", e))
    }
    fn scan_file(
        &self,
        text: &str,
        path: &str,
        results: &mut Vec<Value>,
        ctx: &RuntimeToolContext,
    ) -> Result<bool> {
        match self.args.output_mode {
            GrepOutputMode::Content => self.scan_content(text, path, results, ctx),
            GrepOutputMode::FilesWithMatches => {
                for line in text.lines() {
                    if self.matches(line, ctx)? {
                        return Ok(self.push(results, json!(path)));
                    }
                }
                Ok(false)
            }
            GrepOutputMode::Count => {
                let mut count = 0;
                for line in text.lines() {
                    count += usize::from(self.matches(line, ctx)?);
                }
                Ok(count > 0 && self.push(results, json!({"path":path,"count":count})))
            }
        }
    }
    fn push(&self, results: &mut Vec<Value>, value: Value) -> bool {
        if results.len() == self.args.limit {
            return true;
        }
        results.push(value);
        false
    }
    fn scan_content(
        &self,
        text: &str,
        path: &str,
        results: &mut Vec<Value>,
        ctx: &RuntimeToolContext,
    ) -> Result<bool> {
        let mut before = std::collections::VecDeque::new();
        let mut lines = text.lines().enumerate();
        while let Some((index, line)) = lines.next() {
            if self.matches(line, ctx)? {
                if results.len() == self.args.limit {
                    return Ok(true);
                }
                let after: Vec<_> = lines
                    .clone()
                    .take(self.args.context)
                    .map(|(_, line)| line)
                    .collect();
                results.push(
                    json!({"path":path,"line":index+1,"text":line,"before":before,"after":after}),
                );
            }
            if self.args.context > 0 {
                if before.len() == self.args.context {
                    before.pop_front();
                }
                before.push_back(line);
            }
        }
        Ok(false)
    }
}
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
