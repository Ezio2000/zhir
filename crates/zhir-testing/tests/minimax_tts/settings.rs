use super::contracts::{config, next_event, open, send_json};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message as Frame};
use zhir_core::{error::Error, model::*};
use zhir_minimax::tts::{
    self, AudioFormat, Emotion, SubtitleGranularity, TimbreWeight, VoiceEffects,
};

/// Test host's seekable export of a completed streaming WAV. No header repair
/// happens inside the model adapter, kernel or durable media manifests.
pub(super) fn wave_export(stream: &[u8]) -> Vec<u8> {
    assert!(stream.len() >= 12 && &stream[..4] == b"RIFF" && &stream[8..12] == b"WAVE");
    assert_eq!(&stream[4..8], &[0xff; 4]);
    let mut offset = 12;
    while offset + 8 <= stream.len() {
        let size = u32::from_le_bytes(stream[offset + 4..offset + 8].try_into().unwrap());
        if &stream[offset..offset + 4] == b"data" {
            assert_eq!(size, u32::MAX);
            let data_size = u32::try_from(stream.len() - offset - 8).unwrap();
            let mut exported = stream.to_vec();
            exported[offset + 4..offset + 8].copy_from_slice(&data_size.to_le_bytes());
            if !data_size.is_multiple_of(2) {
                exported.push(0);
            }
            let riff_size = u32::try_from(exported.len() - 8).unwrap();
            exported[4..8].copy_from_slice(&riff_size.to_le_bytes());
            return exported;
        }
        offset += 8 + size as usize + (size as usize % 2);
    }
    panic!("streaming WAV has no data chunk");
}

#[test]
fn incompatible_audio_and_voice_settings_are_rejected_before_connecting() {
    for case in 0..8 {
        let mut settings = config("ws://127.0.0.1:1");
        match case {
            0 => settings.audio.sample_rate = 48000, // outside this endpoint's settings
            1 => settings.audio.format = AudioFormat::PcmuRaw, // must select 8 kHz
            2 => settings.voice.volume = 0.0,
            3 => settings.voice.latex_read = true, // requires explicit Chinese
            4 => settings.timbre_weights.push(TimbreWeight {
                voice_id: "other".into(),
                weight: 50,
            }),
            5 => {
                settings.voice.voice_id.clear();
                settings.timbre_weights.push(TimbreWeight {
                    voice_id: "other".into(),
                    weight: 0,
                });
            }
            6 => {
                settings.audio.format = AudioFormat::Pcm;
                settings.voice_effects = Some(VoiceEffects::default());
            }
            7 => {
                settings.voice_effects = Some(VoiceEffects {
                    pitch: 101,
                    ..Default::default()
                })
            }
            _ => unreachable!(),
        }
        assert!(
            matches!(tts::model(settings), Err(Error::Invalid(_))),
            "case {case}"
        );
    }
}

#[tokio::test]
async fn provider_settings_and_metadata_keep_their_wire_meaning() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        for mismatch in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut settings = config(&format!("ws://{}", listener.local_addr().unwrap()));
            settings.audio.format = AudioFormat::Pcm;
            settings.voice.emotion = Some(Emotion::Happy);
            settings.voice.english_normalization = true;
            settings.voice.latex_read = true;
            settings.language_boost = Some("Chinese".into());
            settings.subtitles = Some(SubtitleGranularity::Word);
            settings.continuous_sound = true;
            settings.voice.voice_id.clear();
            settings.timbre_weights = vec![TimbreWeight {voice_id:"mix".into(),weight:100}];
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                send_json(&mut socket, json!({"event":"connected_success"})).await;
                let Frame::Text(text) = socket.next().await.unwrap().unwrap() else {panic!()};
                let value: Value = serde_json::from_str(&text).unwrap();
                assert_eq!(value["session_id"], "fixture-session");
                assert_eq!(value["voice_setting"]["emotion"], "happy");
                assert_eq!(value["voice_setting"]["english_normalization"], true);
                assert_eq!(value["voice_setting"]["latex_read"], true);
                assert_eq!(value["timbre_weights"][0]["weight"],100);
                assert_eq!(value["subtitle_enable"],true);
                assert_eq!(value["subtitle_type"],"word");
                assert_eq!(value["continuous_sound"],true);
                assert_eq!(value["audio_setting"]["format"], "pcm");
                assert!(value["audio_setting"].get("bitrate").is_none());
                send_json(&mut socket, json!({"event":"task_started"})).await;
                assert!(matches!(socket.next().await, Some(Ok(Frame::Text(_)))));
                send_json(&mut socket, json!({"event":"sentence_start"})).await;
                send_json(&mut socket, json!({"data":{"audio":"0102"},"extra_info":{"audio_format":if mismatch {"mp3"} else {"pcm"}}})).await;
                let _ = socket.next().await;
                let _ = socket.flush().await;
            });
            let model = tts::model(settings).unwrap();
            let opening = open();
            let cancellation = opening.context.cancellation.clone();
            let mut session = model.open_session(opening).await.unwrap();
            session.control.submit(SessionCommand {id:"start".into(),body:SessionCommandBody::Generate {generation_id:"turn".into(),context_revision:0,input_position:0,profile_revision:0}}).await.unwrap();
            assert!(matches!(next_event(&mut session.events).await.unwrap().unwrap().body, SessionEventBody::Acknowledged { .. }));
            if mismatch {
                assert!(matches!(next_event(&mut session.events).await,Err(Error::Protocol(message)) if message.contains("audio_format")));
                assert!(session.media.output.as_mut().unwrap().receive().await.unwrap().is_none());
            } else {
                let chunk = session.media.output.as_mut().unwrap().receive().await.unwrap().unwrap();
                assert_eq!(chunk.media_type,"audio/pcm;encoding=s16le;rate=32000;channels=1");
                assert_eq!(chunk.bytes,vec![1,2]);
                cancellation.cancel();
                assert!(matches!(next_event(&mut session.events).await,Err(Error::Cancelled)));
            }
            server.await.unwrap();
        }
    }).await.unwrap();
}

#[tokio::test]
async fn rejection_keeps_provider_trace_and_original_error_before_termination() {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model = tts::model(config(&format!("ws://{}",listener.local_addr().unwrap()))).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            send_json(&mut socket,json!({"event":"task_failed","trace_id":"provider-trace","base_resp":{"status_code":1000,"status_msg":"specific synthesis failure"}})).await;
            let _ = socket.next().await;
        });
        let mut session = model.open_session(open()).await.unwrap();
        let event = session.events.receive().await.unwrap().unwrap();
        assert!(matches!(event.body,SessionEventBody::Delta {delta:ModelDelta::ProtocolEvent {data,..},..} if data["trace_id"]=="provider-trace"));
        assert!(matches!(session.events.receive().await,Err(Error::Model(failure)) if failure.code=="minimax_1000" && failure.message=="specific synthesis failure"));
        assert!(session.events.receive().await.unwrap().is_none());
        server.await.unwrap();
    }).await.unwrap();
}
