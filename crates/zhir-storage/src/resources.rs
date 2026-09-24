//! Immutable binary resources. Chunked I/O does not materialize an entire resource.
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use zhir_core::{
    BoxFuture, Result,
    error::{Error, ResourceError},
    message::{Content, Message, Output},
    operation::OperationUpdate,
    resource::*,
    run::{Checkpoint, CommandIntent, State},
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
fn stored_key(reference: &ResourceRef) -> Result<&str> {
    reference.validate()?;
    match &reference.source {
        ResourceSource::Stored { key } => Ok(key),
        _ => Err(Error::Invalid("resource is not stored".into())),
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
            let key = stored_key(&reference)?;
            let stored = self
                .entries
                .lock()
                .expect("resource lock")
                .get(key)
                .cloned()
                .ok_or_else(|| ResourceError::NotFound { id: key.into() })?;
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
    fn delete(&self, reference: ResourceRef) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let key = stored_key(&reference)?;
            self.entries.lock().expect("resource lock").remove(key);
            Ok(())
        })
    }
}
/// Lists every stored resource a checkpoint can reach: message and outcome content in
/// history, completed content, pending context, active media cursors and the media
/// archive chain. Each resource appears once. Reclaiming the others is the host's policy.
pub async fn reachable(
    checkpoint: &Checkpoint,
    store: &dyn ResourceStore,
) -> Result<Vec<ResourceRef>> {
    let mut found = Reachable::default();
    for entry in checkpoint.history.iter() {
        match &entry.message {
            Message::System { content }
            | Message::User { content }
            | Message::External { content } => found.contents(content),
            Message::Assistant { output, .. } => {
                for output in output {
                    match output {
                        Output::Content { content } => {
                            found.contents(std::slice::from_ref(content))
                        }
                        Output::ProviderToolCall { call } => found.contents(&call.output),
                        Output::Delegation { .. } | Output::RuntimeToolCall { .. } => {}
                    }
                }
            }
            Message::DelegationResult { outcome, .. } | Message::RuntimeTool { outcome, .. } => {
                found.contents(outcome.content())
            }
        }
    }
    if let State::Completed { content } = &checkpoint.state {
        found.contents(content);
    }
    for command in &checkpoint.active.commands {
        if let CommandIntent::DelegationContext { content, .. } = &command.intent {
            found.contents(content);
        }
    }
    for operation in checkpoint.active.operations.values() {
        if let Some(OperationUpdate::Context { content }) = &operation.last_update {
            found.contents(content);
        }
    }
    for cursor in checkpoint.active.media.values() {
        found.sealed(store, Some(cursor.sealed.clone())).await?;
    }
    let mut archive = checkpoint.active.session.media_archive.clone();
    while let Some(node) = archive.take() {
        if !found.walk(&node) {
            break;
        }
        let archived: ArchivedMedia = read_node(store, &node).await?;
        found.sealed(store, Some(archived.sealed)).await?;
        archive = archived.previous;
    }
    Ok(found.resources)
}
#[derive(Default)]
struct Reachable {
    keys: std::collections::BTreeSet<String>,
    walked: std::collections::BTreeSet<String>,
    resources: Vec<ResourceRef>,
}
impl Reachable {
    /// Lists a media node and reports whether its links still need following. A node
    /// already listed as message content has not been followed yet.
    fn walk(&mut self, reference: &ResourceRef) -> bool {
        self.insert(reference);
        match &reference.source {
            ResourceSource::Stored { key } => self.walked.insert(key.clone()),
            _ => false,
        }
    }
    fn insert(&mut self, reference: &ResourceRef) -> bool {
        let ResourceSource::Stored { key } = &reference.source else {
            return false;
        };
        let new = self.keys.insert(key.clone());
        if new {
            self.resources.push(reference.clone());
        }
        new
    }
    fn contents(&mut self, content: &[Content]) {
        for reference in content.iter().filter_map(Content::source) {
            self.insert(reference);
        }
    }
    /// Follows a sealed media chain until it reaches a node already listed.
    async fn sealed(
        &mut self,
        store: &dyn ResourceStore,
        mut node: Option<ResourceRef>,
    ) -> Result<()> {
        while let Some(reference) = node.take() {
            if !self.walk(&reference) {
                break;
            }
            let sealed: SealedMedia = read_node(store, &reference).await?;
            self.insert(&sealed.resource);
            node = sealed.previous;
        }
        Ok(())
    }
}
/// Media nodes are small JSON documents; larger values are rejected before decoding.
async fn read_node<T: serde::de::DeserializeOwned>(
    store: &dyn ResourceStore,
    reference: &ResourceRef,
) -> Result<T> {
    const LIMIT: usize = 1 << 20;
    let mut reader = store.open(reference.clone()).await?;
    let mut bytes = Vec::new();
    loop {
        let chunk = reader.read(4096).await?;
        if chunk.is_empty() {
            break;
        }
        bytes.extend_from_slice(&chunk);
        if bytes.len() > LIMIT {
            return Err(Error::Invalid("media node exceeds 1 MiB".into()));
        }
    }
    serde_json::from_slice(&bytes).map_err(|e| Error::Invalid(format!("invalid media node: {e}")))
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
    const FORMAT: &str = "zhir-resources 5\n";
    /// Resources live in subdirectories named by the first two hex digits of their id,
    /// under a root marked with the current format.
    #[derive(Clone)]
    pub struct FilesystemResourceStore {
        root: PathBuf,
    }
    impl FilesystemResourceStore {
        /// Opens or initializes a store. A root with another format marker, or with
        /// resource files outside the shard directories, is rejected.
        pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
            let root = root.into();
            tokio::task::spawn_blocking(move || {
                std::fs::create_dir_all(&root).map_err(io)?;
                let marker = root.join("format");
                let unsupported = || {
                    Error::Storage(
                        "unsupported resource directory format; use a fresh directory".into(),
                    )
                };
                match std::fs::read_to_string(&marker) {
                    Ok(format) if format == FORMAT => {}
                    Ok(_) => return Err(unsupported()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        let mut temp = tempfile::NamedTempFile::new_in(&root).map_err(io)?;
                        temp.write_all(FORMAT.as_bytes()).map_err(io)?;
                        temp.as_file().sync_all().map_err(io)?;
                        match temp.persist_noclobber(&marker) {
                            Ok(_) => sync_dir(&root)?,
                            // Another store initialized the root concurrently.
                            Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => {
                                if std::fs::read_to_string(&marker).map_err(io)? != FORMAT {
                                    return Err(unsupported());
                                }
                            }
                            Err(e) => return Err(io(e.error)),
                        }
                    }
                    Err(e) => return Err(io(e)),
                }
                for entry in std::fs::read_dir(&root).map_err(io)? {
                    if entry
                        .map_err(io)?
                        .path()
                        .extension()
                        .is_some_and(|e| e == "resource")
                    {
                        return Err(unsupported());
                    }
                }
                Ok(Self { root })
            })
            .await
            .map_err(io)?
        }
    }
    fn sync_dir(path: &std::path::Path) -> Result<()> {
        std::fs::File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(io)
    }
    /// `id` is a 64-digit hex key.
    fn resource_path(root: &std::path::Path, id: &str) -> PathBuf {
        root.join(&id[..2]).join(format!("{id}.resource"))
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
                    let path = resource_path(&self.root, &self.id);
                    match temp.persist_noclobber(&path) {
                        Ok(_) => sync_dir(path.parent().expect("shard directory"))?,
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
    fn file_key(reference: &ResourceRef) -> Result<&str> {
        let key = stored_key(reference)?;
        if key.len() != 64 || !key.bytes().all(|c| c.is_ascii_hexdigit()) {
            return Err(Error::Invalid("invalid stored resource key".into()));
        }
        Ok(key)
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
                    let shard = root.join(&id[..2]);
                    if !shard.is_dir() {
                        std::fs::create_dir_all(&shard).map_err(io)?;
                        sync_dir(&root)?;
                    }
                    let mut temp = tempfile::NamedTempFile::new_in(&shard).map_err(io)?;
                    let header = serde_json::to_vec(
                        &serde_json::json!({"version": 5,"media_type":media_type}),
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
                let key = file_key(&reference)?.to_owned();
                tokio::task::spawn_blocking(move || {
                    let mut file =
                        std::fs::File::open(resource_path(&root, &key)).map_err(|e| {
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
                    if header["version"] != 5
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
        fn delete(&self, reference: ResourceRef) -> BoxFuture<'_, Result<()>> {
            let root = self.root.clone();
            Box::pin(async move {
                let key = file_key(&reference)?.to_owned();
                tokio::task::spawn_blocking(move || {
                    let path = resource_path(&root, &key);
                    match std::fs::remove_file(&path) {
                        Ok(()) => sync_dir(path.parent().expect("shard directory")),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(e) => Err(io(e)),
                    }
                })
                .await
                .map_err(io)?
            })
        }
    }
}
#[cfg(feature = "resources-filesystem")]
pub use filesystem::FilesystemResourceStore;
