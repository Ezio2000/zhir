//! Native WebRTC media and DataChannel transport. No provider session semantics.
use crate::native::{Buffered, MediaBudget};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc, watch};
use webrtc::{
    api::{
        APIBuilder, interceptor_registry::register_default_interceptors, media_engine::MediaEngine,
    },
    data_channel::RTCDataChannel,
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        RTCPeerConnection, configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::rtp_codec::{RTCRtpCodecParameters, RTPCodecType},
    track::track_local::{TrackLocal, track_local_static_sample::TrackLocalStaticSample},
};
use zhir_core::{BoxFuture, Result, error::Error};

pub struct AudioPacket {
    pub payload: Vec<u8>,
    pub timestamp: u32,
    pub sequence: u16,
    pub ssrc: u32,
}
pub(crate) struct PeerConfig {
    pub connection: RTCConfiguration,
    pub channel_label: &'static str,
    pub audio_codec: RTCRtpCodecParameters,
    pub event_capacity: usize,
    pub max_event_bytes: usize,
    pub max_audio_bytes: usize,
    pub media_budget: MediaBudget,
}

#[path = "webrtc/connection.rs"]
mod connection;
pub use connection::Connection;
#[path = "rtp.rs"]
mod rtp;
pub use rtp::RtpTimeline;
pub(crate) struct Peer {
    pc: Arc<RTCPeerConnection>,
    channel: Arc<RTCDataChannel>,
    track: Arc<TrackLocalStaticSample>,
    pub events: mpsc::Receiver<String>,
    pub audio: mpsc::UnboundedReceiver<Buffered<AudioPacket>>,
    pub failure: watch::Receiver<Option<Error>>,
    pub connection: watch::Receiver<Connection>,
    ready: watch::Receiver<bool>,
    readers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    max_event_bytes: usize,
}
fn error(e: impl std::fmt::Display) -> Error {
    Error::Protocol(format!("WebRTC: {e}"))
}
impl Peer {
    pub async fn new(config: PeerConfig) -> Result<Self> {
        let mut media = MediaEngine::default();
        media
            .register_codec(config.audio_codec.clone(), RTPCodecType::Audio)
            .map_err(error)?;
        let registry = register_default_interceptors(Registry::new(), &mut media).map_err(error)?;
        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .build();
        let pc = Arc::new(
            api.new_peer_connection(config.connection)
                .await
                .map_err(error)?,
        );
        let (tx, events) = mpsc::channel(config.event_capacity);
        // All entries own reservations from the same budget as native output.
        let (audio_tx, audio) = mpsc::unbounded_channel();
        let (failed, failure) = watch::channel(None);
        let (state_tx, connection) = watch::channel(Connection::new(RTCPeerConnectionState::New));
        let (ready_tx, ready) = watch::channel(false);
        let readers = Arc::new(Mutex::new(Vec::new()));
        let audio_failed = failed.clone();
        let audio_readers = readers.clone();
        pc.on_track(Box::new(move |track, _, _| {
            let tx = audio_tx.clone();
            let failed = audio_failed.clone();
            let readers = audio_readers.clone();
            let budget = config.media_budget.clone();
            Box::pin(async move {
                let task = tokio::spawn(async move {
                    while let Ok((packet, _)) = track.read_rtp().await {
                        if packet.payload.len() > config.max_audio_bytes {
                            failed.send_replace(Some(error("oversized audio packet")));
                            break;
                        }
                        let reservation = match budget.try_reserve(packet.payload.len()) {
                            Ok(reservation) => reservation,
                            Err(error) => {
                                failed.send_replace(Some(error));
                                break;
                            }
                        };
                        if tx
                            .send(Buffered {
                                value: AudioPacket {
                                    payload: packet.payload.to_vec(),
                                    timestamp: packet.header.timestamp,
                                    sequence: packet.header.sequence_number,
                                    ssrc: packet.header.ssrc,
                                },
                                reservation,
                            })
                            .is_err()
                        {
                            failed.send_replace(Some(Error::Uncertain(
                                "WebRTC audio receiver closed".into(),
                            )));
                            break;
                        }
                    }
                });
                readers.lock().await.push(task);
            })
        }));
        pc.on_peer_connection_state_change(Box::new(move |state| {
            let tx = state_tx.clone();
            Box::pin(async move {
                tx.send_if_modified(|connection| {
                    if connection.state == state {
                        return false;
                    }
                    *connection = Connection::new(state);
                    true
                });
            })
        }));
        let channel = match pc.create_data_channel(config.channel_label, None).await {
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
        let channel_failed = failed.clone();
        channel.on_close(Box::new(move || {
            let failed = channel_failed.clone();
            Box::pin(async move {
                failed.send_replace(Some(Error::Uncertain(
                    "WebRTC control channel closed".into(),
                )));
            })
        }));
        channel.on_message(Box::new(move |message| {
            let tx = tx.clone();
            let failed = failed.clone();
            Box::pin(async move {
                let result = if !message.is_string || message.data.len() > config.max_event_bytes {
                    Err(error("invalid control frame"))
                } else {
                    String::from_utf8(message.data.to_vec())
                        .map_err(error)
                        .and_then(|text| {
                            tx.try_send(text).map_err(|_| {
                                Error::Uncertain("WebRTC control receive capacity exceeded".into())
                            })
                        })
                };
                if let Err(e) = result {
                    failed.send_replace(Some(e));
                }
            })
        }));
        let track = Arc::new(TrackLocalStaticSample::new(
            config.audio_codec.capability,
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
            events,
            audio,
            failure,
            connection,
            ready,
            readers,
            max_event_bytes: config.max_event_bytes,
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
    pub fn send(&self, text: &str) -> BoxFuture<'static, Result<()>> {
        let channel = self.channel.clone();
        let text = text.to_owned();
        let limit = self.max_event_bytes;
        Box::pin(async move {
            if text.len() > limit {
                return Err(Error::Invalid(
                    "outgoing WebRTC control frame exceeds configured limit".into(),
                ));
            }
            channel.send_text(text).await.map_err(error)?;
            Ok(())
        })
    }
    pub fn audio(
        &self,
        bytes: Vec<u8>,
        duration: std::time::Duration,
    ) -> BoxFuture<'static, Result<()>> {
        let track = self.track.clone();
        Box::pin(async move {
            track
                .write_sample(&Sample {
                    data: Bytes::from(bytes),
                    duration,
                    ..Default::default()
                })
                .await
                .map_err(error)
        })
    }
    pub async fn close(&self) {
        let _ = self.pc.close().await;
        for task in self.readers.lock().await.drain(..) {
            task.abort();
            let _ = task.await;
        }
    }
}
