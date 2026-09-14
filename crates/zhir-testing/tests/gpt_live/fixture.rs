use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};
use webrtc::{
    api::{APIBuilder, media_engine::MediaEngine},
    media::Sample,
    peer_connection::{
        configuration::RTCConfiguration, sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::{TrackLocal, track_local_static_sample::TrackLocalStaticSample},
};
pub struct Evidence {
    pub commands: Vec<Value>,
    pub audio: usize,
}
pub async fn serve() -> (String, tokio::task::JoinHandle<Evidence>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/calls", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut data = vec![];
        let header_end;
        loop {
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buf[..n]);
            if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                header_end = i + 4;
                break;
            }
        }
        let headers = String::from_utf8_lossy(&data[..header_end]).to_lowercase();
        assert!(headers.contains("authorization: bearer test-secret"));
        assert!(headers.contains("chatgpt-account-id: test-account"));
        let length: usize = headers
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        while data.len() < header_end + length {
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            data.extend_from_slice(&buf[..n]);
        }
        let body: Value = serde_json::from_slice(&data[header_end..]).unwrap();
        assert_eq!(body["session"]["model"], "gpt-live-1-codex");
        let mut media = MediaEngine::default();
        media.register_default_codecs().unwrap();
        let api = APIBuilder::new().with_media_engine(media).build();
        let pc = Arc::new(
            api.new_peer_connection(RTCConfiguration::default())
                .await
                .unwrap(),
        );
        let audio = Arc::new(AtomicUsize::new(0));
        let counter = audio.clone();
        pc.on_track(Box::new(move |track, _, _| {
            let count = counter.clone();
            Box::pin(async move {
                tokio::spawn(async move {
                    while track.read_rtp().await.is_ok() {
                        count.fetch_add(1, Ordering::SeqCst);
                    }
                });
            })
        }));
        let (tx, mut rx) = mpsc::channel(32);
        let (ready_tx, mut ready) = mpsc::channel(1);
        pc.on_data_channel(Box::new(move |channel| {
            let tx = tx.clone();
            let ready_tx = ready_tx.clone();
            Box::pin(async move {
                assert_eq!(channel.label(), "oai-events");
                let opened = channel.clone();
                channel.on_open(Box::new(move || {
                    let opened = opened.clone();
                    let ready_tx = ready_tx.clone();
                    Box::pin(async move {
                        ready_tx.send(opened).await.unwrap();
                    })
                }));
                channel.on_message(Box::new(move |m| {
                    let tx = tx.clone();
                    Box::pin(async move {
                        tx.send(serde_json::from_slice::<Value>(&m.data).unwrap())
                            .await
                            .unwrap();
                    })
                }));
            })
        }));
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".into(),
                clock_rate: 48000,
                channels: 2,
                ..Default::default()
            },
            "audio".into(),
            "fixture".into(),
        ));
        let sender = pc
            .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .unwrap();
        let rtcp = tokio::spawn(async move {
            let mut buf = [0; 1500];
            while sender.read(&mut buf).await.is_ok() {}
        });
        pc.set_remote_description(
            RTCSessionDescription::offer(body["sdp"].as_str().unwrap().into()).unwrap(),
        )
        .await
        .unwrap();
        let answer = pc.create_answer(None).await.unwrap();
        let mut gather = pc.gathering_complete_promise().await;
        pc.set_local_description(answer).await.unwrap();
        gather.recv().await;
        let sdp = pc.local_description().await.unwrap().sdp;
        socket.write_all(format!("HTTP/1.1 201 Created\r\nContent-Type: application/sdp\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",sdp.len(),sdp).as_bytes()).await.unwrap();
        drop(socket);
        let channel = ready.recv().await.unwrap();
        for event in [
            json!({"type":"session.started"}),
            json!({"type":"turn.done","turn":{"id":"user-1","role":"user","transcript":"hello"}}),
            json!({"type":"turn.done","turn":{"id":"assistant-1","role":"assistant","transcript":"hi"}}),
            json!({"type":"delegation.created","item":{"id":"delegate-1","type":"delegation","target":"client","content":[{"type":"input_text","text":"inspect files"}]}}),
        ] {
            channel.send_text(event.to_string()).await.unwrap();
        }
        for _ in 0..4 {
            track
                .write_sample(&Sample {
                    data: bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe]),
                    duration: std::time::Duration::from_millis(20),
                    ..Default::default()
                })
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let mut commands = vec![];
        while let Some(event) = rx.recv().await {
            let done = event["type"] == "session.close";
            commands.push(event);
            if done {
                break;
            }
        }
        channel.send_text(json!({"type":"session.closed","reason":"client_request","usage":{"audio_duration_ms":80}}).to_string()).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        pc.close().await.unwrap();
        rtcp.abort();
        let _ = rtcp.await;
        Evidence {
            commands,
            audio: audio.load(Ordering::SeqCst),
        }
    });
    (endpoint, task)
}
