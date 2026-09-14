//! Native WebRTC media and DataChannel transport. No provider session semantics.
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, watch};
use webrtc::{
    api::{
        APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{MIME_TYPE_OPUS, MediaEngine},
    },
    data_channel::RTCDataChannel,
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        RTCPeerConnection, configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::RTCRtpCodecCapability,
    track::track_local::{TrackLocal, track_local_static_sample::TrackLocalStaticSample},
};
use zhir_core::{Result, error::Error};

pub(crate) enum Frame {
    Event(String),
    Audio { payload: Vec<u8>, timestamp: u32 },
}
pub(crate) struct Peer {
    pc: Arc<RTCPeerConnection>,
    channel: Arc<RTCDataChannel>,
    track: Arc<TrackLocalStaticSample>,
    pub frames: mpsc::Receiver<Frame>,
    pub failure: watch::Receiver<Option<Error>>,
    ready: watch::Receiver<bool>,
    readers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}
fn error(e: impl std::fmt::Display) -> Error {
    Error::Protocol(format!("WebRTC: {e}"))
}
impl Peer {
    pub async fn new(
        capacity: usize,
        max_bytes: usize,
        configuration: RTCConfiguration,
    ) -> Result<Self> {
        let mut media = MediaEngine::default();
        media
            .register_codec(
                webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecParameters {
                    capability: RTCRtpCodecCapability {
                        mime_type: MIME_TYPE_OPUS.into(),
                        clock_rate: 48000,
                        channels: 2,
                        sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
                        rtcp_feedback: vec![],
                    },
                    payload_type: 111,
                    ..Default::default()
                },
                webrtc::rtp_transceiver::rtp_codec::RTPCodecType::Audio,
            )
            .map_err(error)?;
        let registry = register_default_interceptors(Registry::new(), &mut media).map_err(error)?;
        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .build();
        let pc = Arc::new(
            api.new_peer_connection(configuration)
                .await
                .map_err(error)?,
        );
        let (tx, frames) = mpsc::channel(capacity);
        let (failed, failure) = watch::channel(None);
        let (ready_tx, ready) = watch::channel(false);
        let readers = Arc::new(Mutex::new(Vec::new()));
        let audio_tx = tx.clone();
        let audio_failed = failed.clone();
        let audio_readers = readers.clone();
        pc.on_track(Box::new(move |track, _, _| {
            let tx = audio_tx.clone();
            let failed = audio_failed.clone();
            let readers = audio_readers.clone();
            Box::pin(async move {
                let task = tokio::spawn(async move {
                    while let Ok((packet, _)) = track.read_rtp().await {
                        if packet.payload.len() > max_bytes {
                            failed.send_replace(Some(error("oversized audio packet")));
                            break;
                        }
                        if tx
                            .try_send(Frame::Audio {
                                payload: packet.payload.to_vec(),
                                timestamp: packet.header.timestamp,
                            })
                            .is_err()
                        {
                            failed.send_replace(Some(Error::Uncertain(
                                "WebRTC receive capacity exceeded".into(),
                            )));
                            break;
                        }
                    }
                });
                readers.lock().await.push(task);
            })
        }));
        let state_failed = failed.clone();
        pc.on_peer_connection_state_change(Box::new(move |state| {
            let failed = state_failed.clone();
            Box::pin(async move {
                if matches!(
                    state,
                    RTCPeerConnectionState::Failed | RTCPeerConnectionState::Disconnected
                ) {
                    failed.send_replace(Some(Error::Uncertain("WebRTC connection lost".into())));
                }
            })
        }));
        let channel = match pc.create_data_channel("oai-events", None).await {
            Ok(channel) => channel,
            Err(cause) => {
                let _ = pc.close().await;
                return Err(error(cause));
            }
        };
        channel.on_open(Box::new(move || {
            let tx = ready_tx.clone();
            Box::pin(async move {
                tx.send_replace(true);
            })
        }));
        channel.on_message(Box::new(move |message| {
            let tx = tx.clone();
            let failed = failed.clone();
            Box::pin(async move {
                let result = if !message.is_string || message.data.len() > max_bytes {
                    Err(error("invalid control frame"))
                } else {
                    String::from_utf8(message.data.to_vec())
                        .map_err(error)
                        .and_then(|text| {
                            tx.try_send(Frame::Event(text)).map_err(|_| {
                                Error::Uncertain("WebRTC receive capacity exceeded".into())
                            })
                        })
                };
                if let Err(e) = result {
                    failed.send_replace(Some(e));
                }
            })
        }));
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.into(),
                clock_rate: 48000,
                channels: 2,
                ..Default::default()
            },
            "audio".into(),
            "zhir".into(),
        ));
        let sender = match pc
            .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
        {
            Ok(sender) => sender,
            Err(cause) => {
                let _ = pc.close().await;
                return Err(error(cause));
            }
        };
        readers.lock().await.push(tokio::spawn(async move {
            let mut buffer = vec![0; 1500];
            while sender.read(&mut buffer).await.is_ok() {}
        }));
        Ok(Self {
            pc,
            channel,
            track,
            frames,
            failure,
            ready,
            readers,
        })
    }
    pub async fn offer(&self) -> Result<String> {
        let offer = self.pc.create_offer(None).await.map_err(error)?;
        let mut gathering = self.pc.gathering_complete_promise().await;
        self.pc.set_local_description(offer).await.map_err(error)?;
        let _ = gathering.recv().await;
        Ok(self
            .pc
            .local_description()
            .await
            .ok_or_else(|| error("missing offer"))?
            .sdp)
    }
    pub async fn answer(&mut self, sdp: String) -> Result<()> {
        self.pc
            .set_remote_description(RTCSessionDescription::answer(sdp).map_err(error)?)
            .await
            .map_err(error)?;
        while !*self.ready.borrow() {
            self.ready.changed().await.map_err(error)?;
        }
        Ok(())
    }
    pub async fn send(&self, value: &serde_json::Value) -> Result<()> {
        self.channel
            .send_text(value.to_string())
            .await
            .map_err(error)?;
        Ok(())
    }
    pub async fn audio(&self, bytes: Vec<u8>, duration: std::time::Duration) -> Result<()> {
        self.track
            .write_sample(&Sample {
                data: Bytes::from(bytes),
                duration,
                ..Default::default()
            })
            .await
            .map_err(error)
    }
    pub async fn close(&self) {
        let _ = self.pc.close().await;
        for task in self.readers.lock().await.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }
}
