//! A real protocol bridge exercised through the unchanged kernel, with local wire faults.
#[path = "minimax_tts/contracts.rs"]
mod contracts;
#[path = "minimax_tts/fixture.rs"]
mod fixture;
#[path = "minimax_tts/observer.rs"]
mod observer;

use observer::{Stats, TtsModel};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::watch;
use zhir_core::{
    message::Message,
    resource::{MediaChunk, MediaReceiver, ResourceRef, ResourceStore, SealedMedia},
    run::{Checkpoint, RunMode, State},
};
use zhir_kernel::{RunRequest, Runtime};
use zhir_storage::{MemoryResourceStore, MemoryRunStore};
use zhir_testing::RecordingStore;

struct Evidence {
    checkpoint: Arc<Checkpoint>,
    chunks: Vec<MediaChunk>,
    stats: Stats,
    first_audio_ms: u128,
    elapsed_ms: u128,
    commits: usize,
}

async fn wait_stats(stats: &mut watch::Receiver<Stats>, predicate: impl Fn(&Stats) -> bool) {
    loop {
        if predicate(&stats.borrow()) {
            return;
        }
        assert!(
            stats.changed().await.is_ok(),
            "adapter stopped before expected event: {:?}",
            *stats.borrow()
        );
    }
}
async fn read_resource(store: &dyn ResourceStore, reference: ResourceRef) -> Vec<u8> {
    let mut reader = store.open(reference).await.unwrap();
    let mut bytes = vec![];
    loop {
        let part = reader.read(4096).await.unwrap();
        if part.is_empty() {
            return bytes;
        }
        bytes.extend(part);
    }
}

async fn exercise(
    endpoint: String,
    key: String,
    model: String,
    interrupt: bool,
    chunk_limit: usize,
) -> Evidence {
    let (model, mut stats) = TtsModel::new(endpoint, key, model);
    let resources = Arc::new(MemoryResourceStore::new());
    let store = Arc::new(RecordingStore::new(Arc::new(MemoryRunStore::new())));
    let runtime = Runtime::builder(Arc::new(model))
        .resources(resources.clone())
        .store(store.clone())
        .defaults(|mut options| {
            options.limits.max_media_chunk_bytes = chunk_limit;
            options.limits.max_buffered_media_bytes = 2 * chunk_limit;
            options
        })
        .build()
        .unwrap();
    let mut invocation = runtime
        .start(
            RunRequest::new([Message::user("你好。这是会话测试。请听下一句。")])
                .mode(RunMode::Interactive),
        )
        .unwrap();
    let control = invocation.control();
    let mut output = invocation.media_output().unwrap();
    let (delivered, mut delivery) = watch::channel(0_usize);
    let started = Instant::now();
    let receive = async {
        let mut chunks = vec![];
        let mut first_audio_ms = None;
        while let Some(chunk) = output.receive().await.unwrap() {
            // Prove durability at delivery time, not just at eventual completion.
            let cursor = store
                .commits()
                .into_iter()
                .find_map(|commit| {
                    commit
                        .checkpoint
                        .active
                        .media
                        .get(&format!("output:{}:{}", chunk.stream_id, chunk.epoch))
                        .filter(|cursor| cursor.sequence == chunk.sequence)
                        .cloned()
                })
                .expect("media escaped before its checkpoint commit");
            let manifest: SealedMedia =
                serde_json::from_slice(&read_resource(resources.as_ref(), cursor.sealed).await)
                    .unwrap();
            assert_eq!(manifest.sequence, chunk.sequence);
            assert_eq!(manifest.epoch, chunk.epoch);
            assert_eq!(manifest.end, chunk.end);
            assert_eq!(
                read_resource(resources.as_ref(), manifest.resource).await,
                chunk.bytes
            );
            if !chunk.bytes.is_empty() {
                first_audio_ms.get_or_insert(started.elapsed().as_millis());
            }
            chunks.push(chunk);
            delivered.send_replace(chunks.len());
            // Exercise downstream backpressure independently of the socket reader.
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
        (chunks, first_audio_ms.unwrap_or_default())
    };
    let commands = async {
        wait_stats(&mut stats, |s| s.starts == 1).await;
        while *delivery.borrow() == 0 {
            delivery.changed().await.unwrap();
        }
        if interrupt {
            control.interrupt().await.unwrap();
            wait_stats(&mut stats, |s| s.cancellations == 1).await;
        }
        // Two pieces without terminal punctuation: EndInput must flush their tail.
        control.input(Message::user("继续"), "test").await.unwrap();
        control
            .input(Message::user("测试完成"), "test")
            .await
            .unwrap();
        control.end_input().await.unwrap();
    };
    let (completion, (), (chunks, first_audio_ms)) =
        tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(invocation.result(), commands, receive)
        })
        .await
        .expect("TTS session exceeded the test budget");
    let checkpoint = completion.unwrap().into_checkpoint();
    let stats = stats.borrow().clone();
    assert!(
        matches!(checkpoint.state, State::Completed { .. }),
        "{:?}; {:?}",
        checkpoint.state,
        stats
    );
    assert_eq!(
        stats.starts, 1,
        "interrupt must not create another remote task"
    );
    assert_eq!(stats.cancellations, usize::from(interrupt));
    assert_eq!(stats.finishes, 1);
    assert!(checkpoint.active.commands.is_empty());
    assert!(chunks.iter().any(|c| !c.bytes.is_empty()));
    assert!(chunks.last().unwrap().end);
    let mut sequences = BTreeMap::new();
    for chunk in &chunks {
        let (next, ended) = sequences
            .entry((&chunk.stream_id, chunk.epoch))
            .or_insert((0, false));
        assert!(!*ended, "chunk arrived after its sentence ended");
        assert_eq!(chunk.sequence, *next);
        *next += 1;
        *ended = chunk.end;
    }
    if interrupt {
        assert!(chunks.iter().any(|c| c.epoch == 1 && !c.bytes.is_empty()));
        assert_eq!(checkpoint.active.session.epoch, 1);
    } else {
        assert!(sequences.values().all(|(_, ended)| *ended));
        assert_eq!(
            chunks.iter().map(|c| c.bytes.len()).sum::<usize>(),
            stats.wire_audio_bytes
        );
    }
    // Both the full wire checkpoint and every immutable manifest remain readable.
    let encoded = zhir_core::wire::encode_checkpoint(&checkpoint).unwrap();
    let restored = zhir_core::wire::decode_checkpoint(&encoded).unwrap();
    assert_eq!(restored.revision, checkpoint.revision);
    for cursor in restored.active.media.values() {
        let mut reference = Some(cursor.sealed.clone());
        let mut previous_sequence = None;
        while let Some(current) = reference {
            let node: SealedMedia =
                serde_json::from_slice(&read_resource(resources.as_ref(), current).await).unwrap();
            if let Some(previous) = previous_sequence {
                assert!(node.sequence < previous);
            }
            previous_sequence = Some(node.sequence);
            reference = node.previous;
        }
    }
    let commits = store.verify_traces().unwrap();
    Evidence {
        checkpoint,
        chunks,
        stats,
        first_audio_ms,
        elapsed_ms: started.elapsed().as_millis(),
        commits,
    }
}

#[tokio::test]
async fn websocket_tts_preserves_fragments_and_drains_tail_before_completion() {
    let (endpoint, server) = fixture::serve(fixture::Fault::None).await;
    let result = exercise(endpoint, String::new(), "fixture".into(), false, 4).await;
    assert_eq!(
        result
            .chunks
            .iter()
            .flat_map(|c| c.bytes.clone())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );
    assert_eq!(
        server.await.unwrap(),
        vec![
            "task_start",
            "task_continue",
            "task_continue",
            "task_continue",
            "task_finish"
        ]
    );
}

#[tokio::test]
async fn websocket_tts_interrupt_keeps_one_session_and_rejects_late_old_epoch_audio() {
    let (endpoint, server) = fixture::serve(fixture::Fault::None).await;
    let result = exercise(endpoint, String::new(), "fixture".into(), true, 4).await;
    assert_eq!(
        result
            .chunks
            .iter()
            .filter(|c| c.epoch == 1)
            .flat_map(|c| c.bytes.clone())
            .collect::<Vec<_>>(),
        vec![4, 5, 6, 7]
    );
    assert!(!result.chunks.iter().any(|c| c.bytes.contains(&0xee)));
    assert_eq!(
        server.await.unwrap(),
        vec![
            "task_start",
            "task_continue",
            "task_cancel",
            "task_continue",
            "task_continue",
            "task_finish"
        ]
    );
}

#[tokio::test]
async fn websocket_tts_slow_consumer_drains_beyond_the_media_byte_budget() {
    let (endpoint, server) = fixture::serve(fixture::Fault::Burst).await;
    let result = exercise(endpoint, String::new(), "fixture".into(), false, 4).await;
    assert_eq!(result.chunks.len(), 27);
    assert_eq!(result.stats.wire_audio_bytes, 76);
    server.await.unwrap();
}

#[tokio::test]
async fn websocket_tts_disconnect_is_uncertain_and_bad_audio_is_not_committed() {
    for fault in [
        fixture::Fault::Disconnect,
        fixture::Fault::BadAudio,
        fixture::Fault::OddAudio,
        fixture::Fault::OversizedAudio,
    ] {
        let (endpoint, server) = fixture::serve(fault).await;
        let (model, _) = TtsModel::new(endpoint, String::new(), "fixture".into());
        let runtime = Runtime::builder(Arc::new(model))
            .resources(Arc::new(MemoryResourceStore::new()))
            .build()
            .unwrap();
        let mut invocation = runtime
            .start(RunRequest::new([Message::user("test")]))
            .unwrap();
        let checkpoint = tokio::time::timeout(Duration::from_secs(5), invocation.result())
            .await
            .unwrap()
            .unwrap()
            .into_checkpoint();
        match fault {
            fixture::Fault::Disconnect => {
                assert!(matches!(checkpoint.state, State::Suspended { .. }))
            }
            fixture::Fault::BadAudio
            | fixture::Fault::OddAudio
            | fixture::Fault::OversizedAudio => {
                assert!(matches!(checkpoint.state, State::Failed { .. }));
                assert!(checkpoint.active.media.is_empty());
            }
            fixture::Fault::None | fixture::Fault::Burst => unreachable!(),
        }
        server.await.unwrap();
    }
}

#[tokio::test]
#[ignore = "uses MiniMax Token Plan; run explicitly with MINIMAX_API_KEY"]
async fn live_minimax_tts_session() {
    let key = std::env::var("MINIMAX_API_KEY").expect("MINIMAX_API_KEY required");
    let endpoint = std::env::var("MINIMAX_TTS_URL")
        .unwrap_or_else(|_| "wss://api.minimaxi.com/ws/v1/t2a_v2_bidi".into());
    let model = std::env::var("MINIMAX_TTS_MODEL").unwrap_or_else(|_| "speech-2.8-hd".into());
    let directory =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../test-results/minimax-tts");
    std::fs::create_dir_all(&directory).unwrap();
    for interrupt in [false, true] {
        let result = exercise(
            endpoint.clone(),
            key.clone(),
            model.clone(),
            interrupt,
            128 * 1024,
        )
        .await;
        let name = if interrupt { "interrupt" } else { "drain" };
        let mut epoch_bytes: BTreeMap<u64, usize> = BTreeMap::new();
        let mut streams = BTreeMap::new();
        for chunk in &result.chunks {
            *epoch_bytes.entry(chunk.epoch).or_default() += chunk.bytes.len();
            let (bytes, complete) = streams
                .entry((chunk.epoch, chunk.stream_id.clone()))
                .or_insert((Vec::new(), false));
            bytes.extend(&chunk.bytes);
            *complete = chunk.end;
        }
        let mut audio = vec![];
        for (index, ((epoch, stream_id), (bytes, complete))) in streams.iter().enumerate() {
            let file = format!("{name}-epoch-{epoch}-sentence-{index}.mp3");
            std::fs::write(directory.join(&file), bytes).unwrap();
            audio.push(json!({"file":file,"epoch":epoch,"stream_id":stream_id,
                "bytes":bytes.len(),"complete":complete}));
        }
        let report = json!({"case":name,"model":model,"state":result.checkpoint.state.kind(),
            "first_audio_ms":result.first_audio_ms,"elapsed_ms":result.elapsed_ms,
            "commits":result.commits,"chunks":result.chunks.len(),"stats":result.stats,
            "bytes_by_epoch":epoch_bytes,"audio":audio});
        std::fs::write(
            directory.join(format!("{name}.json")),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .unwrap();
        println!("{report}");
    }
}
