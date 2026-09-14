use std::{sync::Arc, time::Duration};
use zhir_core::{
    message::Message,
    model::{Capability, SessionCommandBody},
    resource::{MediaChunk, MediaReceiver, ResourceStore, SealedMedia},
    run::RunMode,
};
use zhir_kernel::{RunRequest, Runtime};
use zhir_testing::{RecordingStore, SessionModel};

#[tokio::test]
async fn interrupt_discards_buffered_and_late_output() {
    probe(false).await;
}

#[tokio::test]
async fn late_old_output_preserves_new_epoch_archive_chain() {
    probe(true).await;
}

async fn probe(late_after_new: bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut caps = zhir_testing::model_capabilities();
        caps.features.remove(&Capability::Steering);
        caps.features.extend([Capability::Duplex, Capability::InterruptOutput]);
        let model = Arc::new(SessionModel::new(caps, move |_, mut peer| async move {
            let start = peer.commands.recv().await.unwrap();
            let SessionCommandBody::StartTurn { turn_id, .. } = &start.body else {
                panic!("expected start");
            };
            let turn_id = turn_id.clone();
            peer.acknowledge(&start, None).await?;
            for sequence in 0..2 {
                peer.media_output.send(MediaChunk {
                    stream_id: "voice".into(), turn_id: turn_id.clone(), epoch: 0,
                    sequence, timestamp_us: sequence * 1000, media_type: "audio/pcm".into(),
                    bytes: vec![1; 4], end: false,
                }).await?;
            }
            while let Some(command) = peer.commands.recv().await {
                peer.acknowledge(&command, None).await?;
                if matches!(command.body, SessionCommandBody::InterruptOutput { .. }) {
                    let frames = if late_after_new {
                        vec![(0, 2), (1, 0), (0, 3), (1, 1)]
                    } else {
                        vec![(0, 2), (1, 0)]
                    };
                    for (epoch, sequence) in frames {
                        peer.media_output.send(MediaChunk {
                            stream_id: "voice".into(), turn_id: turn_id.clone(), epoch,
                            sequence, timestamp_us: 3000, media_type: "audio/pcm".into(),
                            bytes: vec![2; 4], end: false,
                        }).await?;
                    }
                }
            }
            Ok(())
        }));
        let store = Arc::new(RecordingStore::new(Arc::new(zhir_storage::MemoryRunStore::new())));
        let resources = Arc::new(zhir_storage::MemoryResourceStore::new());
        let runtime = Runtime::builder(model).store(store.clone())
            .resources(resources.clone()).build().unwrap();
        let mut invocation = runtime.start(RunRequest::new(vec![Message::user("speak")])
            .mode(RunMode::Interactive)).unwrap();
        let mut output = invocation.media_output().unwrap();
        invocation.start();
        loop {
            if store.commits().iter().any(|c| c.checkpoint.active.media.values()
                .any(|cursor| cursor.sequence == 1)) { break; }
            tokio::task::yield_now().await;
        }
        let receipt = invocation.control().interrupt_output().await.unwrap();
        assert!(store.commits().iter().any(|c| c.checkpoint.revision == receipt.revision
            && c.checkpoint.active.session.output_epoch == 1));
        let chunk = output.receive().await.unwrap().unwrap();
        assert_eq!(chunk.epoch, 1, "buffered or late old output was delivered");
        assert_eq!(chunk.sequence, 0);
        if late_after_new {
            let second = output.receive().await.unwrap().unwrap();
            assert_eq!((second.epoch, second.sequence), (1, 1));
            let reference = store.commits().last().unwrap().checkpoint.active.media["output:voice:1"].sealed.clone();
            let mut reader = resources.open(reference).await.unwrap();
            let mut bytes = Vec::new();
            loop {
                let part = reader.read(4096).await.unwrap();
                if part.is_empty() { break; }
                bytes.extend(part);
            }
            let sealed: SealedMedia = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(sealed.sequence, 1);
            assert!(sealed.previous.is_some(), "late epoch 0 frame severed epoch 1 archive chain: sequence 1 has no link to sequence 0");
        }
        invocation.control().cancel();
        let _ = invocation.result().await;
    }).await.unwrap();
}
