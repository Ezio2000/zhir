use super::workspace::*;
use crate::common::{failure, spec};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::Path;
use std::{path::PathBuf, sync::Arc};
use zhir_core::{
    Result,
    error::Error,
    tool::{RuntimeTool, RuntimeToolContext},
};
use zhir_tools::function;
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
