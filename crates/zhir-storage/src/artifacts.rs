//! Immutable artifact stores. Reusing a key with different contents is an error.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    artifact::{ArtifactContent, ArtifactRef, ArtifactStore},
    error::ArtifactError,
};

fn validate(key: &str, content: &ArtifactContent) -> Result<()> {
    if key.is_empty() || content.mime_type.is_empty() {
        return Err(ArtifactError::Invalid {
            id: key.into(),
            message: "key and MIME type must be nonempty".into(),
        }
        .into());
    }
    Ok(())
}
fn validate_reference(
    reference: &ArtifactRef,
    content: ArtifactContent,
) -> Result<ArtifactContent> {
    if reference.mime_type != content.mime_type {
        return Err(ArtifactError::Invalid {
            id: reference.id.clone(),
            message: "MIME type mismatch".into(),
        }
        .into());
    }
    Ok(content)
}
#[derive(Clone, Default)]
pub struct MemoryArtifactStore {
    entries: Arc<Mutex<BTreeMap<String, ArtifactContent>>>,
}
impl MemoryArtifactStore {
    pub fn new() -> Self {
        Self::default()
    }
}
impl ArtifactStore for MemoryArtifactStore {
    fn put(&self, key: String, content: ArtifactContent) -> BoxFuture<'_, Result<ArtifactRef>> {
        Box::pin(async move {
            validate(&key, &content)?;
            let mut entries = self.entries.lock().expect("artifact lock");
            if entries.get(&key).is_some_and(|old| old != &content) {
                return Err(ArtifactError::Conflict { id: key }.into());
            }
            let reference = ArtifactRef {
                id: key.clone(),
                mime_type: content.mime_type.clone(),
            };
            entries.insert(key, content);
            Ok(reference)
        })
    }
    fn get(&self, reference: ArtifactRef) -> BoxFuture<'_, Result<ArtifactContent>> {
        Box::pin(async move {
            let content = self
                .entries
                .lock()
                .expect("artifact lock")
                .get(&reference.id)
                .cloned()
                .ok_or_else(|| ArtifactError::NotFound {
                    id: reference.id.clone(),
                })?;
            validate_reference(&reference, content)
        })
    }
}

#[cfg(feature = "artifacts-filesystem")]
mod filesystem {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        io::Write,
        path::{Path, PathBuf},
    };
    #[derive(Clone)]
    pub struct FilesystemArtifactStore {
        root: PathBuf,
    }
    fn io(operation: &str, error: impl std::fmt::Display) -> zhir_core::error::Error {
        ArtifactError::Io {
            operation: operation.into(),
            message: error.to_string(),
        }
        .into()
    }
    fn read(root: &Path, id: &str) -> Result<ArtifactContent> {
        let bytes = std::fs::read(root.join(format!("{id}.json"))).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                ArtifactError::NotFound { id: id.into() }.into()
            } else {
                io("read", e)
            }
        })?;
        serde_json::from_slice(&bytes).map_err(|e| {
            ArtifactError::Invalid {
                id: id.into(),
                message: e.to_string(),
            }
            .into()
        })
    }
    impl FilesystemArtifactStore {
        pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
            let root = root.into();
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&root).map_err(|e| io("create directory", e))?;
                let root = std::fs::canonicalize(root).map_err(|e| io("resolve directory", e))?;
                Ok(Self { root })
            })
            .await
            .map_err(|e| io("open worker", e))?
        }
    }
    impl ArtifactStore for FilesystemArtifactStore {
        fn put(&self, key: String, content: ArtifactContent) -> BoxFuture<'_, Result<ArtifactRef>> {
            let root = self.root.clone();
            Box::pin(async move {
                validate(&key, &content)?;
                tokio::task::spawn_blocking(move || {
                    let id = format!("{:x}", Sha256::digest(key.as_bytes()));
                    let bytes = serde_json::to_vec(&content).map_err(|e| io("encode", e))?;
                    let mut temp = tempfile::NamedTempFile::new_in(&root)
                        .map_err(|e| io("create temporary file", e))?;
                    temp.write_all(&bytes).map_err(|e| io("write", e))?;
                    temp.as_file().sync_all().map_err(|e| io("sync file", e))?;
                    match temp.persist_noclobber(root.join(format!("{id}.json"))) {
                        Ok(_) => {}
                        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                            if read(&root, &id)? != content {
                                return Err(ArtifactError::Conflict { id }.into());
                            }
                        }
                        Err(e) => return Err(io("publish", e.error)),
                    }
                    std::fs::File::open(&root)
                        .and_then(|f| f.sync_all())
                        .map_err(|e| io("sync directory", e))?;
                    Ok(ArtifactRef {
                        id,
                        mime_type: content.mime_type,
                    })
                })
                .await
                .map_err(|e| io("put worker", e))?
            })
        }
        fn get(&self, reference: ArtifactRef) -> BoxFuture<'_, Result<ArtifactContent>> {
            let root = self.root.clone();
            Box::pin(async move {
                if reference.id.len() != 64 || !reference.id.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return Err(ArtifactError::Invalid {
                        id: reference.id,
                        message: "invalid file artifact identity".into(),
                    }
                    .into());
                }
                tokio::task::spawn_blocking(move || {
                    validate_reference(&reference, read(&root, &reference.id)?)
                })
                .await
                .map_err(|e| io("get worker", e))?
            })
        }
    }
}
#[cfg(feature = "artifacts-filesystem")]
pub use filesystem::FilesystemArtifactStore;
