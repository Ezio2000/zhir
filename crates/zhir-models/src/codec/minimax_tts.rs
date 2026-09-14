//! MiniMax task semantics. The WebSocket driver does not interpret these phases.
use crate::{
    minimax::tts::{AudioSettings, TtsConfig, VoiceSettings},
    streaming::minimax_tts::{Audio, protocol},
    transport::websocket::WireMessage,
    websocket::{Action, WebSocketAdapter, WebSocketProtocol},
};
use serde_json::{Value, json};
use std::sync::Arc;
use zhir_core::{
    Result,
    error::{Error, Failure},
    message::{Content, Message},
    model::*,
    profile::NegotiatedProfile,
};

struct Settings {
    model: String,
    voice: VoiceSettings,
    audio: AudioSettings,
}
pub(crate) struct Adapter(Arc<Settings>);
impl Adapter {
    pub fn new(config: TtsConfig) -> Self {
        Self(Arc::new(Settings {
            model: config.model,
            voice: config.voice,
            audio: config.audio,
        }))
    }
}
impl WebSocketAdapter for Adapter {
    fn capabilities(&self) -> CapabilitySet {
        capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        negotiate(request)
    }
    fn open(&self, open: &SessionOpen) -> Result<Box<dyn WebSocketProtocol>> {
        if open.recovery.is_some() || open.after_sequence.is_some() {
            return Err(protocol("transport does not support session recovery"));
        }
        Ok(Box::new(Session {
            config: self.0.clone(),
            phase: Phase::Connecting,
            turn: None,
            audio: Audio::new(open.output_epoch, open.limits.max_media_chunk_bytes),
        }))
    }
}
enum Phase {
    Connecting,
    Ready,
    Starting { command_id: String, text: String },
    Speaking,
    Interrupting(String),
    Finishing(String),
    Finished,
}
struct Session {
    config: Arc<Settings>,
    phase: Phase,
    turn: Option<String>,
    audio: Audio,
}
impl WebSocketProtocol for Session {
    fn commands_allowed(&self) -> bool {
        matches!(self.phase, Phase::Ready | Phase::Speaking | Phase::Finished)
    }
    fn finished(&self) -> bool {
        matches!(self.phase, Phase::Finished)
    }
    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>> {
        if command.id.is_empty() {
            return Err(protocol("empty command identity"));
        }
        match command.body {
            SessionCommandBody::StartTurn { turn_id, request }
                if matches!(self.phase, Phase::Ready) =>
            {
                if turn_id.is_empty() {
                    return Err(protocol("empty turn identity"));
                }
                negotiate(&request)?;
                let text = latest_text(&request)?;
                self.turn = Some(turn_id);
                self.phase = Phase::Starting {
                    command_id: command.id,
                    text,
                };
                Ok(vec![send(
                    json!({"event":"task_start", "model":self.config.model,
                        "voice_setting":{"voice_id":self.config.voice.voice_id, "speed":self.config.voice.speed,
                            "vol":self.config.voice.volume,"pitch":self.config.voice.pitch},
                        "audio_setting":{"sample_rate":self.config.audio.sample_rate, "bitrate":self.config.audio.bitrate,
                            "format":"mp3","channel":self.config.audio.channels}
                    }),
                )])
            }
            SessionCommandBody::Input { message } if matches!(self.phase, Phase::Speaking) => {
                // There is no per-input remote ack. Acceptance means the adapter sent it,
                // not that synthesis completed. Never replay it after a disconnect.
                Ok(vec![
                    send(json!({"event":"task_continue","text":user_text(&message)?})),
                    ack(command.id),
                ])
            }
            SessionCommandBody::InterruptOutput { turn_id }
                if matches!(self.phase, Phase::Speaking) =>
            {
                if self.turn.as_ref() != Some(&turn_id) {
                    return Err(protocol("interrupt turn mismatch"));
                }
                self.phase = Phase::Interrupting(command.id);
                Ok(vec![send(json!({"event":"task_cancel"}))])
            }
            SessionCommandBody::EndInput if matches!(self.phase, Phase::Speaking) => {
                self.phase = Phase::Finishing(command.id);
                Ok(vec![send(json!({"event":"task_finish"}))])
            }
            SessionCommandBody::Close => Ok(vec![
                ack(command.id),
                Action::Event(SessionEventBody::Closed),
                Action::Close,
            ]),
            _ => Err(protocol("unsupported command or command order")),
        }
    }
    fn receive(&mut self, message: WireMessage) -> Result<Vec<Action>> {
        let WireMessage::Text(text) = message else {
            return Err(protocol("expected a JSON text frame"));
        };
        let value: Value =
            serde_json::from_str(&text).map_err(|_| protocol("invalid JSON event"))?;
        let object = value
            .as_object()
            .ok_or_else(|| protocol("event must be a JSON object"))?;
        if let Some(base) = object.get("base_resp") {
            let code = base
                .get("status_code")
                .and_then(Value::as_i64)
                .ok_or_else(|| protocol("invalid service status code"))?;
            if code == 2205 {
                return Err(Error::Uncertain(
                    "MiniMax TTS input queue rejected an uncorrelated command (2205)".into(),
                ));
            }
            if code != 0 {
                return Err(Error::Model(Failure::new(
                    format!("minimax_{code}"),
                    "MiniMax TTS rejected the task",
                )));
            }
        }
        let event = optional_string(object, "event")?;
        let audio = match object.get("data") {
            Some(data) => optional_string(
                data.as_object()
                    .ok_or_else(|| protocol("invalid event data"))?,
                "audio",
            )?,
            None => None,
        };
        let mut actions = vec![];
        // Decode audio before terminal markers so a final payload cannot lose its tail.
        if let Some(hex) = audio
            && !hex.is_empty()
        {
            if !matches!(
                self.phase,
                Phase::Speaking | Phase::Interrupting(_) | Phase::Finishing(_)
            ) {
                return Err(protocol("audio outside a synthesis task"));
            }
            actions.push(Action::Media(
                self.audio.push(
                    self.turn
                        .as_deref()
                        .ok_or_else(|| protocol("missing turn"))?,
                    hex,
                )?,
            ));
        }
        match event {
            Some("connected_success") if matches!(self.phase, Phase::Connecting) => {
                self.phase = Phase::Ready
            }
            Some("task_started") if matches!(self.phase, Phase::Starting { .. }) => {
                let Phase::Starting { command_id, text } =
                    std::mem::replace(&mut self.phase, Phase::Speaking)
                else {
                    unreachable!()
                };
                actions.extend([
                    send(json!({"event":"task_continue","text":text})),
                    ack(command_id),
                ]);
            }
            Some("task_canceled") if matches!(self.phase, Phase::Interrupting(_)) => {
                let Phase::Interrupting(id) = std::mem::replace(&mut self.phase, Phase::Speaking)
                else {
                    unreachable!()
                };
                // Until this remote barrier, all in-flight audio keeps its old epoch.
                self.audio.cancel()?;
                actions.push(ack(id));
            }
            Some("task_finished") if matches!(self.phase, Phase::Finishing(_)) => {
                let Phase::Finishing(id) = std::mem::replace(&mut self.phase, Phase::Finished)
                else {
                    unreachable!()
                };
                self.audio.finish()?;
                actions.extend([
                    ack(id),
                    Action::Event(SessionEventBody::TurnFinished {
                        turn_id: self.turn.clone().ok_or_else(|| protocol("missing turn"))?,
                        disposition: TurnDisposition::Finished,
                        model_id: Some(self.config.model.clone()),
                        response_id: value
                            .get("session_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        finish_reason: Some("task_finished".into()),
                        usage: Default::default(),
                        provider_data: Value::Null,
                        effective: Default::default(),
                    }),
                ]);
            }
            Some("sentence_start")
                if matches!(
                    self.phase,
                    Phase::Speaking | Phase::Interrupting(_) | Phase::Finishing(_)
                ) =>
            {
                self.audio.start()?
            }
            Some("sentence_end")
                if matches!(
                    self.phase,
                    Phase::Speaking | Phase::Interrupting(_) | Phase::Finishing(_)
                ) =>
            {
                self.end_sentence(&mut actions)?
            }
            // Both named and unnamed audio events belong to the current bidi protocol.
            // An empty event name is invalid; absent names are documented for audio.
            // A final fragment can carry metadata and an empty audio field.
            Some("task_continued")
                if audio.is_some()
                    && matches!(
                        self.phase,
                        Phase::Speaking | Phase::Interrupting(_) | Phase::Finishing(_)
                    ) => {}
            None if audio.is_some()
                && matches!(
                    self.phase,
                    Phase::Speaking | Phase::Interrupting(_) | Phase::Finishing(_)
                ) => {}
            _ => return Err(protocol("unexpected event or event order")),
        }
        actions.insert(
            0,
            Action::Observe(ModelDelta::ProtocolEvent {
                output_index: 0,
                data: value,
            }),
        );
        Ok(actions)
    }
}
impl Session {
    fn end_sentence(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        let turn = self
            .turn
            .as_deref()
            .ok_or_else(|| protocol("missing turn"))?;
        actions.push(Action::Media(self.audio.end(turn)?));
        Ok(())
    }
}
fn send(value: Value) -> Action {
    Action::Send(WireMessage::Text(value.to_string()))
}
fn ack(command_id: String) -> Action {
    Action::Event(SessionEventBody::Acknowledged {
        command_id,
        recovery: None,
    })
}
fn latest_text(request: &ModelRequest) -> Result<String> {
    let message = request
        .messages
        .iter()
        .rev()
        .find(|m| matches!(m, Message::User { .. }))
        .ok_or_else(|| Error::Invalid("MiniMax TTS needs a user text message".into()))?;
    user_text(message)
}
fn user_text(message: &Message) -> Result<String> {
    let Message::User { content } = message else {
        return Err(Error::Invalid("MiniMax TTS accepts user text only".into()));
    };
    if !content.iter().all(|c| matches!(c, Content::Text { .. })) {
        return Err(Error::Invalid("MiniMax TTS accepts text only".into()));
    }
    let text: String = content.iter().filter_map(Content::as_text).collect();
    if text.trim().is_empty() || text.chars().count() > 10000 {
        return Err(Error::Invalid(
            "MiniMax TTS input must contain 1–10000 characters".into(),
        ));
    }
    Ok(text)
}
fn validate_request(request: &ModelRequest) -> Result<()> {
    if !request.profile.extensions.is_empty()
        || request.profile.generation != GenerationProfile::default()
    {
        return Err(Error::Invalid(
            "MiniMax TTS uses TtsConfig; generation and extension overrides are unsupported".into(),
        ));
    }
    latest_text(request)?;
    Ok(())
}

fn optional_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<&'a str>> {
    match object.get(key) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(protocol("invalid event field type")),
    }
}

fn capabilities() -> CapabilitySet {
    CapabilitySet {
        input_modalities: vec!["text".into()],
        output_modalities: vec!["audio".into()],
        features: [
            Capability::Streaming,
            Capability::Duplex,
            Capability::Steering,
            Capability::InterruptOutput,
        ]
        .into(),
        tool_choices: vec!["auto".into(), "none".into()],
        constraints: Default::default(),
        extensions: Default::default(),
    }
}

fn negotiate(request: &ModelRequest) -> Result<NegotiatedProfile> {
    validate_request(request)?;
    zhir_policies::negotiation::negotiate(request, &capabilities())
}
