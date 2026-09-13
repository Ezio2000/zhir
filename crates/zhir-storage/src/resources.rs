//! Immutable binary resources. Chunked I/O does not materialize an entire resource.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::{Error, ResourceError},
    resource::*,
};
fn validate(key: &str, media_type: &str) -> Result<()> {
    if key.is_empty() || media_type.is_empty() {
        Err(Error::Invalid(
            "resource key and media type required".into(),
        ))
    } else {
        Ok(())
    }
}
fn reference(id: String, key: String, media_type: String) -> ResourceRef {
    ResourceRef {
        id,
        media_type,
        name: None,
        source: ResourceSource::Stored { key },
        metadata: BTreeMap::new(),
    }
}
#[derive(Clone, PartialEq)]
struct Stored {
    media_type: String,
    chunks: Vec<Arc<[u8]>>,
}
#[derive(Clone, Default)]
pub struct MemoryResourceStore {
    entries: Arc<Mutex<BTreeMap<String, Stored>>>,
}
impl MemoryResourceStore {
    pub fn new() -> Self {
        Self::default()
    }
}
struct MemoryWriter {
    store: MemoryResourceStore,
    key: String,
    media_type: String,
    chunks: Vec<Arc<[u8]>>,
    sequence: u64,
}
impl ResourceWriter for MemoryWriter {
    fn append(&mut self, sequence: u64, bytes: Vec<u8>) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if sequence != self.sequence {
                return Err(Error::Invalid("resource chunk sequence mismatch".into()));
            }
            self.chunks.push(bytes.into());
            self.sequence += 1;
            Ok(())
        })
    }
    fn finish(self: Box<Self>) -> BoxFuture<'static, Result<ResourceRef>> {
        Box::pin(async move {
            let stored = Stored {
                media_type: self.media_type.clone(),
                chunks: self.chunks,
            };
            let mut entries = self.store.entries.lock().expect("resource lock");
            if let Some(old) = entries.get(&self.key) {
                if old.media_type != stored.media_type
                    || !old
                        .chunks
                        .iter()
                        .flat_map(|c| c.iter())
                        .eq(stored.chunks.iter().flat_map(|c| c.iter()))
                {
                    return Err(ResourceError::Conflict { id: self.key }.into());
                }
            } else {
                entries.insert(self.key.clone(), stored);
            }
            Ok(reference(self.key.clone(), self.key, self.media_type))
        })
    }
}
struct MemoryReader {
    chunks: Vec<Arc<[u8]>>,
    index: usize,
    offset: usize,
}
impl ResourceReader for MemoryReader {
    fn read(&mut self, max_bytes: usize) -> BoxFuture<'_, Result<Vec<u8>>> {
        Box::pin(async move {
            if max_bytes == 0 {
                return Err(Error::Invalid("read size must be positive".into()));
            }
            let mut out = Vec::new();
            while self.index < self.chunks.len() && out.len() < max_bytes {
                let chunk = &self.chunks[self.index];
                let count = (chunk.len() - self.offset).min(max_bytes - out.len());
                out.extend_from_slice(&chunk[self.offset..self.offset + count]);
                self.offset += count;
                if self.offset == chunk.len() {
                    self.index += 1;
                    self.offset = 0;
                }
            }
            Ok(out)
        })
    }
}
impl ResourceStore for MemoryResourceStore {
    fn create(
        &self,
        key: String,
        media_type: String,
    ) -> BoxFuture<'_, Result<Box<dyn ResourceWriter>>> {
        Box::pin(async move {
            validate(&key, &media_type)?;
            Ok(Box::new(MemoryWriter {
                store: self.clone(),
                key,
                media_type,
                chunks: vec![],
                sequence: 0,
            }) as Box<dyn ResourceWriter>)
        })
    }
    fn open(&self, reference: ResourceRef) -> BoxFuture<'_, Result<Box<dyn ResourceReader>>> {
        Box::pin(async move {
            reference.validate()?;
            let ResourceSource::Stored { key } = &reference.source else {
                return Err(Error::Invalid("resource is not stored".into()));
            };
            let stored = self
                .entries
                .lock()
                .expect("resource lock")
                .get(key)
                .cloned()
                .ok_or_else(|| ResourceError::NotFound { id: key.clone() })?;
            if stored.media_type != reference.media_type {
                return Err(Error::Invalid("resource media type mismatch".into()));
            }
            Ok(Box::new(MemoryReader {
                chunks: stored.chunks,
                index: 0,
                offset: 0,
            }) as Box<dyn ResourceReader>)
        })
    }
}
#[cfg(feature = "resources-filesystem")]
mod filesystem {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        io::{Read, Seek, SeekFrom, Write},
        path::PathBuf,
    };
    fn io(error: impl std::fmt::Display) -> Error {
        ResourceError::Io {
            operation: "resource I/O".into(),
            message: error.to_string(),
        }
        .into()
    }
    #[derive(Clone)]
    pub struct FilesystemResourceStore {
        root: PathBuf,
    }
    impl FilesystemResourceStore {
        pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
            let root = root.into();
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&root).map_err(io)?;
                Ok(Self { root })
            })
            .await
            .map_err(io)?
        }
    }
    struct Writer {
        root: PathBuf,
        id: String,
        media_type: String,
        temp: Arc<Mutex<Option<tempfile::NamedTempFile>>>,
        sequence: u64,
    }
    impl ResourceWriter for Writer {
        fn append(&mut self, sequence: u64, bytes: Vec<u8>) -> BoxFuture<'_, Result<()>> {
            Box::pin(async move {
                if sequence != self.sequence {
                    return Err(Error::Invalid("resource chunk sequence mismatch".into()));
                }
                let temp = self.temp.clone();
                tokio::task::spawn_blocking(move || {
                    temp.lock()
                        .expect("writer lock")
                        .as_mut()
                        .ok_or_else(|| Error::Invalid("resource writer finished".into()))?
                        .write_all(&bytes)
                        .map_err(io)
                })
                .await
                .map_err(io)??;
                self.sequence += 1;
                Ok(())
            })
        }
        fn finish(self: Box<Self>) -> BoxFuture<'static, Result<ResourceRef>> {
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    let temp = self
                        .temp
                        .lock()
                        .expect("writer lock")
                        .take()
                        .ok_or_else(|| Error::Invalid("resource writer finished".into()))?;
                    temp.as_file().sync_all().map_err(io)?;
                    let path = self.root.join(format!("{}.resource", self.id));
                    match temp.persist_noclobber(&path) {
                        Ok(_) => {
                            std::fs::File::open(&self.root)
                                .and_then(|f| f.sync_all())
                                .map_err(io)?;
                        }
                        Err(mut error)
                            if error.error.kind() == std::io::ErrorKind::AlreadyExists =>
                        {
                            let mut previous = std::fs::File::open(&path).map_err(io)?;
                            error
                                .file
                                .as_file_mut()
                                .seek(SeekFrom::Start(0))
                                .map_err(io)?;
                            if digest(&mut previous)? != digest(error.file.as_file_mut())? {
                                return Err(ResourceError::Conflict { id: self.id }.into());
                            }
                        }
                        Err(error) => return Err(io(error.error)),
                    }
                    Ok(reference(self.id.clone(), self.id, self.media_type))
                })
                .await
                .map_err(io)?
            })
        }
    }
    fn digest(reader: &mut impl Read) -> Result<Vec<u8>> {
        let mut hash = Sha256::new();
        let mut buffer = [0; 65536];
        loop {
            let count = reader.read(&mut buffer).map_err(io)?;
            if count == 0 {
                break;
            }
            hash.update(&buffer[..count]);
        }
        Ok(hash.finalize().to_vec())
    }
    struct Reader(Arc<Mutex<std::fs::File>>);
    impl ResourceReader for Reader {
        fn read(&mut self, max_bytes: usize) -> BoxFuture<'_, Result<Vec<u8>>> {
            Box::pin(async move {
                if max_bytes == 0 {
                    return Err(Error::Invalid("read size must be positive".into()));
                }
                let file = self.0.clone();
                tokio::task::spawn_blocking(move || {
                    let mut bytes = vec![0; max_bytes];
                    let size = file
                        .lock()
                        .expect("reader lock")
                        .read(&mut bytes)
                        .map_err(io)?;
                    bytes.truncate(size);
                    Ok(bytes)
                })
                .await
                .map_err(io)?
            })
        }
    }
    impl ResourceStore for FilesystemResourceStore {
        fn create(
            &self,
            key: String,
            media_type: String,
        ) -> BoxFuture<'_, Result<Box<dyn ResourceWriter>>> {
            let root = self.root.clone();
            Box::pin(async move {
                validate(&key, &media_type)?;
                tokio::task::spawn_blocking(move || {
                    let id = format!("{:x}", Sha256::digest(key.as_bytes()));
                    let mut temp = tempfile::NamedTempFile::new_in(&root).map_err(io)?;
                    let header = serde_json::to_vec(
                        &serde_json::json!({"version":2,"media_type":media_type}),
                    )
                    .map_err(io)?;
                    temp.write_all(&(header.len() as u64).to_le_bytes())
                        .map_err(io)?;
                    temp.write_all(&header).map_err(io)?;
                    Ok(Box::new(Writer {
                        root,
                        id,
                        media_type,
                        temp: Arc::new(Mutex::new(Some(temp))),
                        sequence: 0,
                    }) as Box<dyn ResourceWriter>)
                })
                .await
                .map_err(io)?
            })
        }
        fn open(&self, reference: ResourceRef) -> BoxFuture<'_, Result<Box<dyn ResourceReader>>> {
            let root = self.root.clone();
            Box::pin(async move {
                reference.validate()?;
                tokio::task::spawn_blocking(move || {
                    let ResourceSource::Stored { key } = reference.source else {
                        return Err(Error::Invalid("resource is not stored".into()));
                    };
                    if key.len() != 64 || !key.bytes().all(|c| c.is_ascii_hexdigit()) {
                        return Err(Error::Invalid("invalid stored resource key".into()));
                    }
                    let mut file = std::fs::File::open(root.join(format!("{key}.resource")))
                        .map_err(|e| {
                            if e.kind() == std::io::ErrorKind::NotFound {
                                ResourceError::NotFound { id: key.clone() }.into()
                            } else {
                                io(e)
                            }
                        })?;
                    let mut size = [0; 8];
                    file.read_exact(&mut size).map_err(io)?;
                    let length = u64::from_le_bytes(size);
                    if length > 65536 {
                        return Err(Error::Invalid("invalid resource header".into()));
                    }
                    let mut header = vec![0; length as usize];
                    file.read_exact(&mut header).map_err(io)?;
                    let header: serde_json::Value = serde_json::from_slice(&header).map_err(io)?;
                    if header["version"] != 2
                        || header["media_type"].as_str() != Some(&reference.media_type)
                    {
                        return Err(Error::Invalid(
                            "unsupported resource format or media type mismatch".into(),
                        ));
                    }
                    Ok(Box::new(Reader(Arc::new(Mutex::new(file)))) as Box<dyn ResourceReader>)
                })
                .await
                .map_err(io)?
            })
        }
    }
}
#[cfg(feature = "resources-filesystem")]
pub use filesystem::FilesystemResourceStore;
