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
        let mut next = self.current.as_ref().clone();
        next.active.media.insert(
            key,
            StreamCursor {
                sequence: chunk.sequence,
                epoch: chunk.epoch,
                sealed: reference,
            },
        );
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
    pub(super) fn input_media(&mut self, packet: Packet) -> Result<()> {
        let chunk = &packet.chunk;
        chunk.validate(self.current.options.limits.max_media_chunk_bytes)?;
        if chunk.epoch < self.current.active.session.epoch {
            return Ok(());
        }
        if chunk.epoch != self.current.active.session.epoch
            || self.current.active.session.turn_id.as_ref() != Some(&chunk.turn_id)
        {
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
        turn_id: chunk.turn_id.clone(),
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
                        previous.insert(key, reference.clone());
                        let (ack, rx) = oneshot::channel();
                        tx.send(Work::MediaReady(chunk.clone(), reference, ack))
                            .await
                            .map_err(|_| Error::Cancelled)?;
                        if rx.await.map_err(|_| Error::Cancelled)? {
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
