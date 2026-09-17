//! A real protocol bridge exercised through the native kernel, with local wire faults.
#[path = "minimax_tts/contracts.rs"]
mod contracts;
#[path = "minimax_tts/fixture.rs"]
mod fixture;
#[path = "minimax_tts/native.rs"]
mod native;
#[path = "minimax_tts/observer.rs"]
mod observer;
#[path = "minimax_tts/settings.rs"]
mod settings;

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
    resource::{ArchivedMedia, MediaChunk, MediaReceiver, ResourceRef, ResourceStore, SealedMedia},
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
    config: zhir_minimax::tts::TtsConfig,
    interrupt: bool,
    flush: bool,
    chunk_limit: usize,
) -> Evidence {
    let (model, mut stats) = TtsModel::new(config);
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
            let commits = store.commits();
            let key = format!("output:{}:{}", chunk.stream_id, chunk.epoch);
            let reference = if chunk.end {
                let mut found = None;
                for commit in &commits {
                    if let Some(reference) = &commit.checkpoint.active.session.media_archive {
                        let node: ArchivedMedia = serde_json::from_slice(
                            &read_resource(resources.as_ref(), reference.clone()).await,
                        )
                        .unwrap();
                        if node.stream_key == key && node.complete {
                            found = Some(node.sealed);
                            break;
                        }
                    }
                }
                found
            } else {
                commits.iter().find_map(|commit| {
                    commit
                        .checkpoint
                        .active
                        .media
                        .get(&key)
                        .filter(|cursor| cursor.sequence == chunk.sequence)
                        .map(|cursor| cursor.sealed.clone())
                })
            }
            .expect("media escaped before its checkpoint commit");
            let manifest: SealedMedia =
                serde_json::from_slice(&read_resource(resources.as_ref(), reference).await)
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
            control.interrupt_output().await.unwrap();
            wait_stats(&mut stats, |s| s.cancellations == 1).await;
        }
        // Two pieces without terminal punctuation: SealUserInput must flush their tail.
        control.input(Message::user("继续"), "test").await.unwrap();
        control
            .input(Message::user("测试完成"), "test")
            .await
            .unwrap();
        if flush {
            for fragment in [" ", "\n"] {
                control
                    .input(Message::user(fragment), "test")
                    .await
                    .unwrap();
            }
            control.flush_input().await.unwrap();
            wait_stats(&mut stats, |s| s.flushes == 1).await;
            assert_eq!(
                stats.borrow().finishes,
                0,
                "flush closed the synthesis task"
            );
            control
                .input(Message::user("再次继续"), "test")
                .await
                .unwrap();
        }
        control.seal_user_input().await.unwrap();
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
    assert_eq!(stats.flushes, usize::from(flush));
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
        assert_eq!(checkpoint.active.session.output_epoch, 1);
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
    assert!(restored.active.media.is_empty());
    let mut archived = restored.active.session.media_archive.clone();
    let mut streams = 0;
    while let Some(reference) = archived {
        let archive: ArchivedMedia =
            serde_json::from_slice(&read_resource(resources.as_ref(), reference).await).unwrap();
        archived = archive.previous;
        let mut reference = Some(archive.sealed);
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
        streams += 1;
    }
    assert!(streams > 0);
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
    let result = exercise(
        TtsModel::config(endpoint, String::new(), "fixture".into()),
        false,
        false,
        4,
    )
    .await;
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
    let result = exercise(
        TtsModel::config(endpoint, String::new(), "fixture".into()),
        true,
        false,
        4,
    )
    .await;
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
    let result = exercise(
        TtsModel::config(endpoint, String::new(), "fixture".into()),
        false,
        false,
        4,
    )
    .await;
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
        let (model, _) = TtsModel::new(TtsModel::config(endpoint, String::new(), "fixture".into()));
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
    // Pace distinct test sessions against subscription RPM limits. A failed
    // synthesis is never retried or replayed by the production adapter.
    let mut next_case = tokio::time::Instant::now();
    use zhir_minimax::tts::{
        AudioFormat, Emotion, SoundEffect, SubtitleGranularity, TimbreWeight, VoiceEffects,
    };
    for (format, extension, rate, mime) in [
        (AudioFormat::Mp3, "mp3", 32000, "audio/mpeg"),
        (
            AudioFormat::Pcm,
            "pcm",
            32000,
            "audio/pcm;encoding=s16le;rate=32000;channels=1",
        ),
        (AudioFormat::Wav, "wav", 32000, "audio/wav"),
        (AudioFormat::Flac, "flac", 32000, "audio/flac"),
        (
            AudioFormat::PcmuRaw,
            "pcmu",
            8000,
            "audio/PCMU;rate=8000;channels=1",
        ),
        (AudioFormat::PcmuWav, "pcmu.wav", 8000, "audio/wav"),
    ] {
        for (case, interrupt, flush) in [
            ("drain", false, false),
            ("interrupt", true, false),
            ("flush", false, true),
        ] {
            tokio::time::sleep_until(next_case).await;
            next_case = tokio::time::Instant::now() + Duration::from_secs(15);
            let name = format!("{extension}-{case}");
            let mut config = TtsModel::config(endpoint.clone(), key.clone(), model.clone());
            config.audio.format = format;
            config.audio.sample_rate = rate;
            if format == AudioFormat::Mp3 {
                config.voice.voice_id.clear();
                config.timbre_weights = vec![
                    TimbreWeight {
                        voice_id: "male-qn-qingse".into(),
                        weight: 50,
                    },
                    TimbreWeight {
                        voice_id: "female-tianmei".into(),
                        weight: 50,
                    },
                ];
                config.voice.emotion = Some(Emotion::Calm);
                config.voice.english_normalization = true;
                config.voice.latex_read = true;
                config.voice_effects = Some(VoiceEffects {
                    pitch: 10,
                    intensity: 10,
                    timbre: 10,
                    sound_effects: Some(SoundEffect::Robotic),
                });
                config.subtitles = Some(SubtitleGranularity::WordStreaming);
                config.continuous_sound = true;
            }
            let result = exercise(config, interrupt, flush, 128 * 1024).await;
            let mut epoch_bytes: BTreeMap<u64, usize> = BTreeMap::new();
            let mut streams = BTreeMap::new();
            for chunk in &result.chunks {
                assert_eq!(chunk.media_type, mime);
                *epoch_bytes.entry(chunk.epoch).or_default() += chunk.bytes.len();
                let (bytes, complete) = streams
                    .entry((chunk.epoch, chunk.stream_id.clone()))
                    .or_insert((Vec::new(), false));
                bytes.extend(&chunk.bytes);
                *complete = chunk.end;
            }
            let mut audio = vec![];
            for (index, ((epoch, stream_id), (bytes, complete))) in streams.iter().enumerate() {
                let file = format!("{name}-epoch-{epoch}-sentence-{index}.{extension}");
                std::fs::write(directory.join(&file), bytes).unwrap();
                if *complete {
                    assert!(!bytes.is_empty());
                    if matches!(format, AudioFormat::Wav | AudioFormat::PcmuWav) {
                        // Wire WAV uses unknown RIFF/data lengths because the header
                        // precedes synthesis. Preserve that stream and finalize a
                        // seekable host export; production media stays byte-exact.
                        std::fs::write(directory.join(format!("{file}.stream")), bytes).unwrap();
                        std::fs::write(directory.join(&file), settings::wave_export(bytes))
                            .unwrap();
                    }
                    let mut decode = std::process::Command::new("ffmpeg");
                    decode.args(["-hide_banner", "-v", "error", "-xerror"]);
                    if matches!(format, AudioFormat::Pcm | AudioFormat::PcmuRaw) {
                        decode.args([
                            "-f",
                            if format == AudioFormat::Pcm {
                                "s16le"
                            } else {
                                "mulaw"
                            },
                            "-ar",
                            &rate.to_string(),
                            "-ac",
                            "1",
                        ]);
                    }
                    let result = decode
                        .arg("-i")
                        .arg(directory.join(&file))
                        .args(["-f", "null", "-"])
                        .output()
                        .expect("real audio tests require ffmpeg");
                    assert!(
                        result.status.success(),
                        "{file}: {}",
                        String::from_utf8_lossy(&result.stderr)
                    );
                }
                audio.push(json!({"file":file,"epoch":epoch,"stream_id":stream_id,
                "bytes":bytes.len(),"complete":complete,"wave_lengths_finalized":*complete && matches!(format, AudioFormat::Wav | AudioFormat::PcmuWav)}));
            }
            let report = json!({"case":name,"model":model,"state":result.checkpoint.state.kind(),
            "first_audio_ms":result.first_audio_ms,"elapsed_ms":result.elapsed_ms,
            "commits":result.commits,"chunks":result.chunks.len(),"stats":result.stats,
            "bytes_by_epoch":epoch_bytes,"audio":audio,"format":format,"media_type":mime,"sample_rate":rate,"complete_streams_decoded":true});
            std::fs::write(
                directory.join(format!("{name}.json")),
                serde_json::to_vec_pretty(&report).unwrap(),
            )
            .unwrap();
            println!("{report}");
        }
    }
}
