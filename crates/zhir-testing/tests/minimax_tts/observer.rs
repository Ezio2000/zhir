use zhir_minimax::tts::{self, TtsConfig};
// Test-only observation around the production adapter; no wire translation lives here.
use std::sync::Arc;
use tokio::sync::watch;
use zhir_core::{BoxFuture, Result, model::*, profile::NegotiatedProfile};
use zhir_models::{WebSocketModel, credentials::StaticCredential};

#[derive(Clone, Default, Debug, serde::Serialize)]
pub struct Stats {
    pub starts: usize,
    pub cancellations: usize,
    pub finishes: usize,
    pub flushes: usize,
    pub wire_audio_bytes: usize,
    pub events: Vec<String>,
    pub failure: Option<String>,
}
pub struct TtsModel(WebSocketModel, watch::Sender<Stats>);
impl TtsModel {
    pub fn config(endpoint: String, key: String, model: String) -> TtsConfig {
        let mut config = TtsConfig::new(
            model,
            "male-qn-qingse",
            Arc::new(StaticCredential::new("Bearer", key)),
        );
        config.connection.url = endpoint;
        config.language_boost = Some("Chinese".into());
        config.pronunciation_dictionary = vec!["测试/(ce4)(shi4)".into()];
        config
    }
    pub fn new(config: TtsConfig) -> (Self, watch::Receiver<Stats>) {
        let (stats, receiver) = watch::channel(Stats::default());
        (Self(tts::model(config).unwrap(), stats), receiver)
    }
}
impl Model for TtsModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.0.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.0.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let mut session = self.0.open_session(open).await?;
            session.output = Box::new(Events(session.output, self.1.clone()));
            Ok(session)
        })
    }
}
struct Events(Box<dyn SessionReceiver>, watch::Sender<Stats>);
impl SessionReceiver for Events {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            let result = self.0.receive().await;
            if let Ok(Some(SessionEvent {
                body:
                    SessionEventBody::Delta {
                        delta: ModelDelta::ProtocolEvent { data, .. },
                        ..
                    },
                ..
            })) = &result
            {
                let event = data
                    .get("event")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                self.1.send_modify(|s| {
                    if s.events.len() < 512 {
                        s.events.push(event.to_owned());
                    }
                    match event {
                        "task_started" => s.starts += 1,
                        "task_canceled" => s.cancellations += 1,
                        "task_finished" => s.finishes += 1,
                        "task_flushed" => s.flushes += 1,
                        _ => (),
                    }
                    s.wire_audio_bytes += data
                        .pointer("/data/audio")
                        .and_then(serde_json::Value::as_str)
                        .map_or(0, |hex| hex.len() / 2);
                });
            }
            if let Err(error) = &result {
                self.1.send_modify(|s| s.failure = Some(error.to_string()));
            }
            result
        })
    }
}
