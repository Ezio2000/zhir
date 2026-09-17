use super::*;

impl Engine {
    pub(super) async fn seal_cursor(
        &mut self,
        direction: &str,
        chunk: &MediaChunk,
        reference: zhir_core::resource::ResourceRef,
    ) -> Result<()> {
        let key = media_key(direction, chunk);
        if self
            .current
            .active
            .media
            .get(&key)
            .is_some_and(|c| chunk.sequence <= c.sequence)
        {
            return Err(Error::Protocol("media sequence did not increase".into()));
        }
        if !self.current.active.media.contains_key(&key) {
            self.ensure_new_stream(&key).await?;
        }
        let mut next = self.current.as_ref().clone();
        if chunk.end {
            self.archive_media(&mut next, key.clone(), reference.clone(), true)
                .await?;
            next.active.media.remove(&key);
        } else {
            next.active.media.insert(
                key,
                StreamCursor {
                    sequence: chunk.sequence,
                    epoch: chunk.epoch,
                    sealed: reference,
                },
            );
        }
        self.commit(
            next,
            Fact::Media {
                stream_id: chunk.stream_id.clone(),
                sequence: chunk.sequence,
            },
            HistoryDelta::Unchanged,
        )
        .await
    }

    async fn ensure_new_stream(&self, key: &str) -> Result<()> {
        let mut reference = self.current.active.session.media_archive.clone();
        let Some(resources) = &self.config.resources else {
            return Ok(());
        };
        while let Some(node) = reference {
            self.check()?;
            let mut reader = resources.open(node).await?;
            let mut bytes = Vec::new();
            loop {
                let part = reader.read(4096).await?;
                if part.is_empty() {
                    break;
                }
                if bytes.len() + part.len() > 1024 * 1024 {
                    return Err(Error::Protocol("oversized media archive node".into()));
                }
                bytes.extend(part);
            }
            let node: zhir_core::resource::ArchivedMedia = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Protocol("invalid media archive node".into()))?;
            if node.stream_key == key {
                return Err(Error::Protocol("media after stream end".into()));
            }
            reference = node.previous;
        }
        Ok(())
    }
    pub(super) async fn archive_media(
        &self,
        next: &mut Checkpoint,
        stream_key: String,
        sealed: zhir_core::resource::ResourceRef,
        complete: bool,
    ) -> Result<()> {
        let resources = self
            .config
            .resources
            .as_ref()
            .ok_or_else(|| Error::Invalid("media archive requires resource storage".into()))?;
        let node = zhir_core::resource::ArchivedMedia {
            stream_key,
            sealed,
            complete,
            previous: next.active.session.media_archive.clone(),
        };
        let mut writer = resources
            .create(new_id(), "application/vnd.zhir.archived-media+json".into())
            .await?;
        writer
            .append(
                0,
                serde_json::to_vec(&node).map_err(|e| Error::Invalid(e.to_string()))?,
            )
            .await?;
        next.active.session.media_archive = Some(writer.finish().await?);
        Ok(())
    }
    pub(super) fn input_media(&mut self, packet: Packet) -> Result<()> {
        let chunk = &packet.chunk;
        chunk.validate(self.current.options.limits.max_media_chunk_bytes)?;
        if chunk.epoch != 0 || self.current.active.session.id != chunk.session_id {
            return Err(Error::Invalid("foreign media turn or epoch".into()));
        }
        let resources = self
            .config
            .resources
            .clone()
            .ok_or_else(|| Error::Invalid("media input requires resource storage".into()))?;
        let input = self
            .model_media
            .clone()
            .ok_or_else(|| Error::Invalid("model has no media input".into()))?;
        let previous = self
            .current
            .active
            .media
            .get(&media_key("input", chunk))
            .map(|cursor| cursor.sealed.clone());
        let tx = self.work_tx.clone();
        self.input_sending = true;
        self.tasks.spawn(async move {
            let result: Result<()> = async {
                let reference = seal_media(resources.as_ref(), &packet.chunk, previous).await?;
                let (ack, rx) = oneshot::channel();
                tx.send(Work::InputReady(packet.chunk.clone(), reference, ack))
                    .await
                    .map_err(|_| Error::Cancelled)?;
                if rx.await.map_err(|_| Error::Cancelled)? {
                    input.send(packet.chunk.clone()).await?;
                }
                Ok(())
            }
            .await;
            drop(packet);
            let _ = tx.send(Work::InputSent(result)).await;
        });
        Ok(())
    }
}

pub(super) fn media_key(direction: &str, chunk: &MediaChunk) -> String {
    format!("{direction}:{}:{}", chunk.stream_id, chunk.epoch)
}

pub(super) async fn seal_media(
    store: &dyn zhir_core::resource::ResourceStore,
    chunk: &MediaChunk,
    previous: Option<zhir_core::resource::ResourceRef>,
) -> Result<zhir_core::resource::ResourceRef> {
    let mut writer = store.create(new_id(), chunk.media_type.clone()).await?;
    writer.append(0, chunk.bytes.clone()).await?;
    let resource = writer.finish().await?;
    let node = zhir_core::resource::SealedMedia {
        stream_id: chunk.stream_id.clone(),
        session_id: chunk.session_id.clone(),
        epoch: chunk.epoch,
        sequence: chunk.sequence,
        timestamp_us: chunk.timestamp_us,
        end: chunk.end,
        resource,
        previous,
    };
    let mut writer = store
        .create(new_id(), "application/vnd.zhir.sealed-media+json".into())
        .await?;
    writer
        .append(
            0,
            serde_json::to_vec(&node).map_err(|e| Error::Invalid(e.to_string()))?,
        )
        .await?;
    writer.finish().await
}

impl Engine {
    pub(super) fn start_media_output(
        &mut self,
        media: Option<Box<dyn zhir_core::resource::MediaReceiver>>,
    ) -> Result<()> {
        if let Some(mut media) = media {
            self.media_pending = true;
            let mut previous: BTreeMap<_, _> = self
                .current
                .active
                .media
                .iter()
                .map(|(key, cursor)| (key.clone(), cursor.sealed.clone()))
                .collect();
            let resources =
                self.config.resources.clone().ok_or_else(|| {
                    Error::Invalid("media session requires a resource store".into())
                })?;
            let tx = self.work_tx.clone();
            let output = self.media_output.clone();
            let limit = self.current.options.limits.max_media_chunk_bytes;
            self.tasks.spawn(async move {
                loop {
                    let result: Result<bool> = async {
                        let Some(chunk) = media.receive().await? else {
                            return Ok(false);
                        };
                        chunk.validate(limit)?;
                        let key = media_key("output", &chunk);
                        let reference =
                            seal_media(resources.as_ref(), &chunk, previous.get(&key).cloned())
                                .await?;
                        let (ack, rx) = oneshot::channel();
                        tx.send(Work::MediaReady(chunk.clone(), reference.clone(), ack))
                            .await
                            .map_err(|_| Error::Cancelled)?;
                        if rx.await.map_err(|_| Error::Cancelled)? {
                            // Only accepted frames may mutate the chain. A late old
                            // epoch must not erase the current generation's links.
                            if chunk.end {
                                previous.remove(&key);
                            } else {
                                previous
                                    .retain(|key, _| key.ends_with(&format!(":{}", chunk.epoch)));
                                previous.insert(key, reference);
                            }
                            output.send(chunk).await?;
                        }
                        Ok(true)
                    }
                    .await;
                    match result {
                        Ok(true) => (),
                        Ok(false) => {
                            let _ = tx.send(Work::MediaDone).await;
                            break;
                        }
                        Err(error) => {
                            let _ = tx.send(Work::MediaError(error)).await;
                            break;
                        }
                    }
                }
            });
        }
        Ok(())
    }
}
