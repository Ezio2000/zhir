#![cfg(feature = "openai-live")]
#[allow(dead_code)]
#[path = "../../zhir-models/src/native/media.rs"]
mod native;
#[path = "../../zhir-models/src/transport/webrtc.rs"]
mod transport;
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
#[tokio::test]
#[ignore = "isolated subscription protocol diagnostic"]
async fn subscription_remaining_probe() {
    let dir = PathBuf::from(std::env::var("ZHIR_PROBE_DIR").unwrap());
    std::fs::write(dir.join("pid"), std::process::id().to_string()).unwrap();
    let auth: Value = serde_json::from_slice(
        &std::fs::read(std::env::var("ZHIR_LIVE_AUTH_JSON").unwrap()).unwrap(),
    )
    .unwrap();
    let client = reqwest::Client::builder()
        .proxy(reqwest::Proxy::all(std::env::var("ZHIR_LIVE_PROXY").unwrap()).unwrap())
        .build()
        .unwrap();
    let mut peer = transport::Peer::new(transport::PeerConfig {
        connection: Default::default(),
        channel_label: "oai-events",
        audio_codec: webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecParameters {
            capability: webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability {
                mime_type: "audio/opus".into(),
                clock_rate: 48000,
                channels: 2,
                sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
                rtcp_feedback: vec![],
            },
            payload_type: 111,
            ..Default::default()
        },
        event_capacity: 8192,
        max_event_bytes: 1024 * 1024,
        max_audio_bytes: 1024 * 1024,
        media_budget: native::MediaBudget::new(16 * 1024 * 1024),
    })
    .await
    .unwrap();
    let session = if dir.join("session.json").exists() {
        serde_json::from_slice(&std::fs::read(dir.join("session.json")).unwrap()).unwrap()
    } else {
        json!({"model":"gpt-live-1-codex","audio":{"output":{"voice":"cove"}},"instructions":"你是语音测试助手。听到用户说话后，请立刻开始从一数到一百，速度慢，每个数字之间稍停顿。","delegation":{"type":"client","ack_filler":false}})
    };
    let offer = peer.offer().await.unwrap();
    let response = client.post("https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas")
        .bearer_auth(auth["tokens"]["access_token"].as_str().unwrap())
        .header("ChatGPT-Account-Id",auth["tokens"]["account_id"].as_str().unwrap())
        .header("OpenAI-Alpha","quicksilver=v2").header("originator","codex_cli_rs").header("x-session-id",uuid::Uuid::new_v4().to_string())
        .json(&json!({"sdp":offer,"session":session})).send().await.unwrap();
    let status = response.status().as_u16();
    let location = response
        .headers()
        .get("location")
        .and_then(|h| h.to_str().ok())
        .map(str::to_owned);
    let body = response.text().await.unwrap();
    std::fs::write(dir.join("created.json"), serde_json::to_vec_pretty(&json!({"status":status,"location":location,"error":if status < 300 {None} else {Some(&body)}})).unwrap()).unwrap();
    assert!(status < 300, "creation rejected");
    tokio::time::timeout(Duration::from_secs(25), peer.answer(body))
        .await
        .unwrap()
        .unwrap();
    let began = tokio::time::Instant::now();
    let mut events = vec![];
    let mut audio = vec![];
    let mut timeline = transport::RtpTimeline::default();
    let mut interval = tokio::time::interval(Duration::from_millis(20));
    let mut command_index = 0;
    let speech: Vec<Vec<u8>> = serde_json::from_slice(
        &std::fs::read(std::env::var("ZHIR_LIVE_INPUT_PACKETS").unwrap()).unwrap(),
    )
    .unwrap();
    let mut speech_index = 0;
    loop {
        tokio::select! {
            _ = peer.failure.changed() => {
                if let Some(error) = &*peer.failure.borrow() {
                    events.push(json!({"failure":error.to_string(),"ms":began.elapsed().as_millis()}));
                    break;
                }
            }
            _ = peer.connection.changed() => {
                let state = peer.connection.borrow();
                events.push(json!({"connection":format!("{:?}",state.state),"usable":state.connected(Duration::from_secs(10)).unwrap_or(false),"deadline":state.deadline(Duration::from_secs(10)).is_some(),"ms":began.elapsed().as_millis()}));
            }
            Some(text) = peer.events.recv() => {
                let event: Value = serde_json::from_str(&text).unwrap();
                events.push(json!({"event":event,"ms":began.elapsed().as_millis()}));
                std::fs::write(dir.join("primary-events.json"),serde_json::to_vec_pretty(&events).unwrap()).unwrap();
                if event["type"] == "session.started" {
                    std::fs::write(dir.join("started.json"),serde_json::to_vec_pretty(&event).unwrap()).unwrap();
                }
                if event["type"] == "session.closed" { break; }
            },
            Some(frame) = peer.audio.recv() => {
                let transport::AudioPacket {timestamp, sequence, ssrc, payload} = frame.value;
                let accepted = timeline.accept(ssrc, sequence, timestamp).unwrap().is_some();
                audio.push(json!({"ms":began.elapsed().as_millis(),"rtp":timestamp,"sequence":sequence,"ssrc":ssrc,"bytes":payload.len(),"accepted":accepted,"ticks":timeline.ticks()}));
                if audio.len().is_multiple_of(50) {
                    std::fs::write(dir.join("primary-audio.json"),serde_json::to_vec_pretty(&audio).unwrap()).unwrap();
                }
            },
            _ = interval.tick() => {
                if began.elapsed() >= Duration::from_secs(150) { break; }
                if dir.join("destroy").exists() { break; }
                let path = dir.join(format!("command-{command_index}.json"));
                if path.exists() {
                    let command: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                    peer.send(&command.to_string()).await.unwrap();
                    command_index += 1;
                }
                let bytes = if dir.join("speak").exists() && speech_index < speech.len() {
                    let bytes = speech[speech_index].clone();
                    speech_index += 1;
                    bytes
                } else { vec![0xf8, 0xff, 0xfe] };
                peer.audio(bytes, Duration::from_millis(20)).await.unwrap();
            }
        }
    }
    peer.close().await;
    std::fs::write(
        dir.join("primary-events.json"),
        serde_json::to_vec_pretty(&events).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.join("primary-audio.json"),
        serde_json::to_vec_pretty(&audio).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir.join("destroyed.json"),
        serde_json::to_vec_pretty(
            &json!({"ms":began.elapsed().as_millis(),"audio_packets":audio.len()}),
        )
        .unwrap(),
    )
    .unwrap();
}
