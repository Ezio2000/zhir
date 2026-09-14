//! Test-only observation around the production adapter; no wire translation lives here.
use std::sync::Arc;
use tokio::sync::watch;
use zhir::models::{
    FunctionDeltaSink, WebSocketModel,
    credentials::StaticCredential,
    minimax::tts::{self, TtsConfig},
};
use zhir_core::{BoxFuture, Result, model::*, profile::NegotiatedProfile};

#[derive(Clone, Default, Debug, serde::Serialize)]
pub struct Stats {
    pub starts: usize,
    pub cancellations: usize,
    pub finishes: usize,
    pub wire_audio_bytes: usize,
    pub events: Vec<String>,
    pub failure: Option<String>,
}
pub struct TtsModel(WebSocketModel, watch::Sender<Stats>);
impl TtsModel {
    pub fn new(endpoint: String, key: String, model: String) -> (Self, watch::Receiver<Stats>) {
        let mut config = TtsConfig::new(
            model,
            "male-qn-qingse",
            Arc::new(StaticCredential::new("Bearer", key)),
        );
        config.connection.url = endpoint;
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
    fn open_session(&self, mut open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let stats = self.1.clone();
            let downstream = open.context.deltas.take();
            open.context.deltas = Some(Arc::new(FunctionDeltaSink::new(move |delta| {
                let stats = stats.clone();
                let downstream = downstream.clone();
                async move {
                    if let ModelDelta::ProtocolEvent { data, .. } = &delta {
                        let event = data
                            .get("event")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("");
                        stats.send_modify(|s| {
                            if s.events.len() < 512 {
                                s.events.push(event.to_owned());
                            }
                            match event {
                                "task_started" => s.starts += 1,
                                "task_canceled" => s.cancellations += 1,
                                "task_finished" => s.finishes += 1,
                                _ => (),
                            }
                            s.wire_audio_bytes += data
                                .pointer("/data/audio")
                                .and_then(serde_json::Value::as_str)
                                .map_or(0, |hex| hex.len() / 2);
                        });
                    }
                    if let Some(sink) = downstream {
                        sink.emit(delta).await?;
                    }
                    Ok(())
                }
            })));
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
            if let Err(error) = &result {
                self.1.send_modify(|s| s.failure = Some(error.to_string()));
            }
            result
        })
    }
}
