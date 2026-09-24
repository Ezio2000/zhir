use super::*;
use zhir_core::resource::{ArchivedMedia, ResourceRef, ResourceStore, SealedChunk, SealedMedia};

impl Engine {
    /// Commits one sealed segment: the cursor advances to its last chunk, or the stream
    /// is archived when that chunk ends it.
    pub(super) async fn seal_cursor(
        &mut self,
        direction: &str,
        chunks: &[MediaChunk],
        reference: ResourceRef,
    ) -> Result<()> {
        let (Some(first), Some(last)) = (chunks.first(), chunks.last()) else {
            return Err(Error::Protocol("empty media segment".into()));
        };
        let key = media_key(direction, first);
        let cursor = self.current.active.media.get(&key);
        if cursor.is_some_and(|c| first.sequence <= c.sequence)
            || chunks.windows(2).any(|w| w[1].sequence <= w[0].sequence)
        {
            return Err(Error::Protocol("media sequence did not increase".into()));
        }
        if cursor.is_none() {
            self.ensure_new_stream(&key).await?;
        }
        let mut next = self.current.as_ref().clone();
        if last.end {
            self.archive_media(&mut next, key.clone(), reference, true)
                .await?;
            next.active.media.remove(&key);
        } else {
            next.active.media.insert(
                key.clone(),
                StreamCursor {
                    sequence: last.sequence,
                    epoch: last.epoch,
                    sealed: reference,
                },
            );
        }
        self.commit(
            next,
            Fact::Media {
                stream_id: last.stream_id.clone(),
                sequence: last.sequence,
            },
            HistoryDelta::Unchanged,
        )
        .await?;
        if last.end {
            self.ended_stream(key);
        }
        Ok(())
    }

    /// Records a stream archived by a committed checkpoint.
    pub(super) fn ended_stream(&mut self, key: String) {
        if let Some(ended) = &mut self.ended_streams {
            ended.insert(key);
        }
    }

    /// Rejects media for an archived stream. The archive chain is read once per engine;
    /// later archives update the in-memory set.
    async fn ensure_new_stream(&mut self, key: &str) -> Result<()> {
        let Some(resources) = self.config.resources.clone() else {
            return Ok(());
        };
        if self.ended_streams.is_none() {
            let mut ended = BTreeSet::new();
            let mut reference = self.current.active.session.media_archive.clone();
            while let Some(node) = reference {
                self.check()?;
                let node: ArchivedMedia = read_node(resources.as_ref(), node)
                    .await
                    .map_err(|_| Error::Protocol("invalid media archive node".into()))?;
                ended.insert(node.stream_key);
                reference = node.previous;
            }
            self.ended_streams = Some(ended);
        }
        if self
            .ended_streams
            .as_ref()
            .is_some_and(|ended| ended.contains(key))
        {
            return Err(Error::Protocol("media after stream end".into()));
        }
        Ok(())
    }
    pub(super) async fn archive_media(
        &self,
        next: &mut Checkpoint,
        stream_key: String,
        sealed: ResourceRef,
        complete: bool,
    ) -> Result<()> {
        let resources = self
            .config
            .resources
            .as_ref()
            .ok_or_else(|| Error::Invalid("media archive requires resource storage".into()))?;
        let node = ArchivedMedia {
            stream_key,
            sealed,
            complete,
            previous: next.active.session.media_archive.clone(),
        };
        next.active.session.media_archive = Some(
            write_node(
                resources.as_ref(),
                "application/vnd.zhir.archived-media+json",
                &node,
            )
            .await?,
        );
        Ok(())
    }
    /// Seals the host packets already queued behind `first` for the same stream as one
    /// segment, commits it once, then sends its chunks to the model in order.
    pub(super) fn input_media(&mut self, first: Packet) -> Result<()> {
        let limits = &self.current.options.limits;
        let (max_chunk, max_packets) = (
            limits.max_media_chunk_bytes,
            limits.max_buffered_media_packets,
        );
        let mut segment = vec![first];
        while segment.len() < max_packets {
            let Ok(packet) = self.media.try_recv() else {
                break;
            };
            if joins(&segment[segment.len() - 1].chunk, &packet.chunk) {
                segment.push(packet);
            } else {
                self.held_input = Some(packet);
                break;
            }
        }
        for packet in &segment {
            packet.chunk.validate(max_chunk)?;
            if packet.chunk.epoch != 0 || self.current.active.session.id != packet.chunk.session_id
            {
                return Err(Error::Invalid("foreign media turn or epoch".into()));
            }
        }
        let resources = self
            .config
            .resources
            .clone()
            .ok_or_else(|| Error::Invalid("media input requires resource storage".into()))?;
        let input = self
            .model_media_input
            .clone()
            .ok_or_else(|| Error::Invalid("model has no media input".into()))?;
        let previous = self
            .current
            .active
            .media
            .get(&media_key("input", &segment[0].chunk))
            .map(|cursor| cursor.sealed.clone());
        let tx = self.work_tx.clone();
        self.input_sending = true;
        self.tasks.spawn(async move {
            let result: Result<()> = async {
                let chunks: Vec<_> = segment.iter().map(|p| p.chunk.clone()).collect();
                let reference = seal_media(resources.as_ref(), &chunks, previous).await?;
                let (ack, rx) = oneshot::channel();
                tx.send(Work::InputReady(chunks, reference, ack))
                    .await
                    .map_err(|_| Error::Cancelled)?;
                if rx.await.map_err(|_| Error::Cancelled)? {
                    for packet in &segment {
                        input.send(packet.chunk.clone()).await?;
                    }
                }
                Ok(())
            }
            .await;
            drop(segment);
            let _ = tx.send(Work::InputSent(result)).await;
        });
        Ok(())
    }
}

pub(super) fn media_key(direction: &str, chunk: &MediaChunk) -> String {
    format!("{direction}:{}:{}", chunk.stream_id, chunk.epoch)
}

/// A queued chunk continues a segment when it belongs to the same stream, epoch and
/// media type and the segment has not ended.
fn joins(last: &MediaChunk, next: &MediaChunk) -> bool {
    !last.end
        && last.stream_id == next.stream_id
        && last.session_id == next.session_id
        && last.epoch == next.epoch
        && last.media_type == next.media_type
}

/// Stores a segment's bytes as one resource, then its immutable node.
pub(super) async fn seal_media(
    store: &dyn ResourceStore,
    chunks: &[MediaChunk],
    previous: Option<ResourceRef>,
) -> Result<ResourceRef> {
    let first = chunks
        .first()
        .ok_or_else(|| Error::Protocol("empty media segment".into()))?;
    let mut writer = store.create(new_id(), first.media_type.clone()).await?;
    let mut sealed = Vec::with_capacity(chunks.len());
    let mut offset = 0;
    for (index, chunk) in chunks.iter().enumerate() {
        let length = chunk.bytes.len() as u64;
        writer.append(index as u64, chunk.bytes.clone()).await?;
        sealed.push(SealedChunk {
            sequence: chunk.sequence,
            timestamp_us: chunk.timestamp_us,
            offset,
            length,
            end: chunk.end,
        });
        offset += length;
    }
    let node = SealedMedia {
        stream_id: first.stream_id.clone(),
        session_id: first.session_id.clone(),
        epoch: first.epoch,
        chunks: sealed,
        resource: writer.finish().await?,
        previous,
    };
    write_node(store, "application/vnd.zhir.sealed-media+json", &node).await
}

async fn write_node(
    store: &dyn ResourceStore,
    media_type: &str,
    node: &impl serde::Serialize,
) -> Result<ResourceRef> {
    let mut writer = store.create(new_id(), media_type.into()).await?;
    writer
        .append(
            0,
            serde_json::to_vec(node).map_err(|e| Error::Invalid(e.to_string()))?,
        )
        .await?;
    writer.finish().await
}

async fn read_node<T: serde::de::DeserializeOwned>(
    store: &dyn ResourceStore,
    reference: ResourceRef,
) -> Result<T> {
    let mut reader = store.open(reference).await?;
    let mut bytes = Vec::new();
    loop {
        let part = reader.read(4096).await?;
        if part.is_empty() {
            break;
        }
        if bytes.len() + part.len() > 1024 * 1024 {
            return Err(Error::Protocol("oversized media node".into()));
        }
        bytes.extend(part);
    }
    serde_json::from_slice(&bytes).map_err(|_| Error::Protocol("invalid media node".into()))
}

impl Engine {
    /// A reader task moves model media into a bounded kernel queue; a sealer task seals
    /// the chunks already queued for one stream as a segment, commits its cursor once
    /// and then delivers the chunks in order. An idle stream seals single chunks.
    pub(super) fn start_media_output(
        &mut self,
        media: Option<Box<dyn zhir_core::resource::MediaReceiver>>,
    ) -> Result<()> {
        let Some(mut media) = media else {
            return Ok(());
        };
        let resources = self
            .config
            .resources
            .clone()
            .ok_or_else(|| Error::Invalid("media session requires a resource store".into()))?;
        self.media_pending = true;
        let mut previous: BTreeMap<_, _> = self
            .current
            .active
            .media
            .iter()
            .map(|(key, cursor)| (key.clone(), cursor.sealed.clone()))
            .collect();
        let limits = &self.current.options.limits;
        let max_packets = limits.max_buffered_media_packets;
        let (queue, mut queued) = crate::invocation::media_pipe(
            limits.max_buffered_media_bytes,
            max_packets,
            limits.max_media_chunk_bytes,
        );
        // The reader's terminal error is reported after every chunk queued before it.
        let failure = Arc::new(std::sync::Mutex::new(None));
        let reader_failure = failure.clone();
        self.tasks.spawn(async move {
            let result: Result<()> = async {
                while let Some(chunk) = media.receive().await? {
                    queue.send(chunk).await?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                *reader_failure.lock().expect("media failure lock") = Some(error);
            }
        });
        let tx = self.work_tx.clone();
        let output = self.media_output.clone();
        self.tasks.spawn(async move {
            let mut held = None;
            let result: Result<()> = async {
                loop {
                    let first = match held.take() {
                        Some(packet) => packet,
                        None => match queued.recv().await {
                            Some(packet) => packet,
                            None => return Ok(()),
                        },
                    };
                    let mut segment = vec![first];
                    while segment.len() < max_packets {
                        let Ok(packet) = queued.try_recv() else {
                            break;
                        };
                        if joins(&segment[segment.len() - 1].chunk, &packet.chunk) {
                            segment.push(packet);
                        } else {
                            held = Some(packet);
                            break;
                        }
                    }
                    let chunks: Vec<MediaChunk> = segment.iter().map(|p| p.chunk.clone()).collect();
                    let last = &chunks[chunks.len() - 1];
                    let key = media_key("output", last);
                    let reference =
                        seal_media(resources.as_ref(), &chunks, previous.get(&key).cloned())
                            .await?;
                    let (ack, rx) = oneshot::channel();
                    let (end, epoch) = (last.end, last.epoch);
                    tx.send(Work::MediaReady(chunks, reference.clone(), ack))
                        .await
                        .map_err(|_| Error::Cancelled)?;
                    if rx.await.map_err(|_| Error::Cancelled)? {
                        // Only accepted segments may mutate the chain. A late old
                        // epoch must not erase the current generation's links.
                        if end {
                            previous.remove(&key);
                        } else {
                            previous.retain(|key, _| key.ends_with(&format!(":{epoch}")));
                            previous.insert(key, reference);
                        }
                        for packet in segment {
                            output.send(packet.chunk).await?;
                        }
                    }
                }
            }
            .await;
            let failure = failure.lock().expect("media failure lock").take();
            let _ = match result.err().or(failure) {
                None => tx.send(Work::MediaDone).await,
                Some(error) => tx.send(Work::MediaError(error)).await,
            };
        });
        Ok(())
    }
}
