//! Filesystem tools over one workspace, with shared file and cancellation primitives.
mod read;
mod search;
mod workspace;
mod write;
pub use read::{list_files, read_file};
pub use search::{glob, grep};
use std::{path::PathBuf, sync::Arc};
pub use write::{edit_file, write_file};
use zhir_core::{Result, tool::RuntimeTool};
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
