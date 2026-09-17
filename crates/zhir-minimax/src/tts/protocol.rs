//! MiniMax task semantics. The WebSocket driver does not interpret these phases.
use super::{
    AudioFormat, TtsConfig,
    audio::{Audio, protocol},
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
use zhir_models::{
    Confirmation,
    websocket::{Action, WebSocketAdapter, WebSocketProtocol, WireMessage},
};

pub(crate) struct Adapter(Arc<TtsConfig>);
impl Adapter {
    pub fn new(config: TtsConfig) -> Self {
        Self(Arc::new(config))
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
            return Err(protocol(
                "MiniMax adapter has no verified session recovery mechanism",
            ));
        }
        Ok(Box::new(Session {
            config: self.0.clone(),
            phase: Phase::Connecting,
            confirmation: Confirmation::default(),
            metadata: Value::Null,
            session_id: open.session_id.clone(),
            generation: None,
            request: open.request.clone(),
            context_revision: open.context_revision,
            input_position: open.input_position,
            audio: Audio::new(
                open.output_epoch,
                open.limits.max_media_chunk_bytes,
                self.0
                    .audio
                    .format
                    .media_type(self.0.audio.sample_rate, self.0.audio.channels),
            ),
        }))
    }
}
enum Phase {
    Connecting,
    Ready,
    Starting { text: String },
    Speaking,
    Interrupting { epoch: u64 },
    Flushing,
    Finishing,
    Finished,
}
struct Session {
    config: Arc<TtsConfig>,
    session_id: String,
    phase: Phase,
    generation: Option<String>,
    request: ModelRequest,
    context_revision: u64,
    input_position: u64,
    audio: Audio,
    confirmation: Confirmation<Option<String>>,
    metadata: Value,
}
impl WebSocketProtocol for Session {
    fn connected(&mut self) -> Result<()> {
        self.confirmation.begin(
            None,
            "connected_success",
            self.config.connection.command_timeout,
        )
    }
    fn commands_allowed(&self) -> bool {
        self.confirmation.pending().is_none()
            && matches!(self.phase, Phase::Ready | Phase::Speaking | Phase::Finished)
    }
    fn finished(&self) -> bool {
        matches!(self.phase, Phase::Finished)
    }
    fn generation_id(&self) -> Option<&str> {
        self.generation.as_deref()
    }
    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.confirmation.deadline()
    }
    fn check_deadline(&self) -> Result<()> {
        self.confirmation.check()
    }
    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>> {
        if command.id.is_empty() {
            return Err(protocol("empty command identity"));
        }
        match command.body {
            SessionCommandBody::Generate {
                generation_id,
                context_revision,
                input_position,
                profile_revision,
            } if matches!(self.phase, Phase::Ready) => {
                if generation_id.is_empty() {
                    return Err(protocol("empty generation identity"));
                }
                if context_revision != self.context_revision
                    || input_position != self.input_position
                    || profile_revision != 0
                {
                    return Err(protocol("generation projection mismatch"));
                }
                let text = latest_text(&self.request)?;
                self.generation = Some(generation_id);
                self.confirmation.begin(
                    Some(command.id),
                    "task_started",
                    self.config.connection.command_timeout,
                )?;
                self.phase = Phase::Starting { text };
                let mut start = json!({"event":"task_start", "model":self.config.model,
                    "session_id":self.session_id,
                    "voice_setting":self.config.voice,
                    "audio_setting":{"sample_rate":self.config.audio.sample_rate,
                        "format":self.config.audio.format,"channel":self.config.audio.channels},
                    "continuous_sound":self.config.continuous_sound
                });
                if self.config.audio.format == AudioFormat::Mp3 {
                    start["audio_setting"]["bitrate"] = json!(self.config.audio.bitrate);
                }
                if !self.config.timbre_weights.is_empty() {
                    start["timbre_weights"] = json!(self.config.timbre_weights);
                }
                if let Some(effects) = &self.config.voice_effects {
                    start["voice_modify"] = json!(effects);
                }
                if let Some(subtitles) = self.config.subtitles {
                    start["subtitle_enable"] = json!(true);
                    start["subtitle_type"] = json!(subtitles);
                }
                if let Some(language) = &self.config.language_boost {
                    start["language_boost"] = json!(language);
                }
                if !self.config.pronunciation_dictionary.is_empty() {
                    start["pronunciation_dict"] =
                        json!({"tone":self.config.pronunciation_dictionary});
                }
                Ok(vec![send(start)])
            }
            SessionCommandBody::Append {
                entry,
                source,
                context_revision,
                input_position,
            } => {
                if context_revision != self.context_revision + 1 {
                    return Err(protocol("non-contiguous context revision"));
                }
                self.context_revision = context_revision;
                self.input_position = input_position;
                if source == AppendSource::Accepted {
                    return Ok(vec![acknowledge(command.id, Acknowledgement::Projection)]);
                }
                if matches!(self.phase, Phase::Ready) {
                    self.request.messages.push(entry.message);
                    return Ok(vec![acknowledge(command.id, Acknowledgement::Projection)]);
                }
                if !matches!(self.phase, Phase::Speaking) {
                    return Err(protocol("input outside synthesis"));
                }
                // There is no per-input remote ack. Acceptance means the adapter sent it,
                // not that synthesis completed. Never replay it after a disconnect.
                Ok(vec![
                    send(json!({"event":"task_continue","text":user_text(&entry.message)?})),
                    acknowledge(command.id, Acknowledgement::Transport),
                ])
            }
            SessionCommandBody::InterruptOutput {
                generation_id,
                output_epoch,
            } if matches!(self.phase, Phase::Speaking) => {
                if self.generation.as_ref() != Some(&generation_id) {
                    return Err(protocol("interrupt generation mismatch"));
                }
                self.audio.validate_epoch(output_epoch)?;
                self.confirmation.begin(
                    Some(command.id),
                    "task_canceled",
                    self.config.connection.command_timeout,
                )?;
                self.phase = Phase::Interrupting {
                    epoch: output_epoch,
                };
                Ok(vec![send(json!({"event":"task_cancel"}))])
            }
            SessionCommandBody::FlushInput if matches!(self.phase, Phase::Speaking) => {
                self.confirmation.begin(
                    Some(command.id),
                    "task_flushed",
                    self.config.connection.command_timeout,
                )?;
                self.phase = Phase::Flushing;
                Ok(vec![send(json!({"event":"task_flush"}))])
            }
            SessionCommandBody::SealUserInput if matches!(self.phase, Phase::Speaking) => {
                self.confirmation.begin(
                    Some(command.id),
                    "task_finished",
                    self.config.connection.command_timeout,
                )?;
                self.phase = Phase::Finishing;
                Ok(vec![send(json!({"event":"task_finish"}))])
            }
            SessionCommandBody::Close => Ok(vec![
                acknowledge(command.id, Acknowledgement::Projection),
                Action::Event(SessionEventBody::Closed {
                    reason: "host_request".into(),
                    provider_data: Value::Null,
                }),
                Action::Close,
            ]),
            _ => Err(protocol("unsupported command or command order")),
        }
    }
    fn receive(&mut self, message: WireMessage) -> Result<Vec<Action>> {
        self.confirmation.check()?;
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
            if code != 0 {
                let message = base
                    .get("status_msg")
                    .and_then(Value::as_str)
                    .ok_or_else(|| protocol("invalid service status message"))?;
                let failure = if code == 2205 {
                    Error::Uncertain(format!(
                        "MiniMax TTS input queue rejected an uncorrelated command (2205): {message}"
                    ))
                } else {
                    Error::Model(Failure::new(format!("minimax_{code}"), message))
                };
                return Ok(vec![
                    Action::Observe(ModelDelta::ProtocolEvent {
                        output_index: 0,
                        data: value,
                    }),
                    Action::Fail(failure),
                ]);
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
        if let Some(metadata) = object.get("extra_info") {
            self.validate_audio_metadata(metadata)?;
            self.metadata = metadata.clone();
        }
        let mut actions = vec![];
        // Decode audio before terminal markers so a final payload cannot lose its tail.
        if let Some(hex) = audio
            && !hex.is_empty()
        {
            if !matches!(
                self.phase,
                Phase::Speaking | Phase::Interrupting { .. } | Phase::Flushing | Phase::Finishing
            ) {
                return Err(protocol("audio outside a synthesis task"));
            }
            actions.push(Action::Media(self.audio.push(&self.session_id, hex)?));
        }
        match event {
            Some("connected_success") if matches!(self.phase, Phase::Connecting) => {
                self.confirmation.complete()?;
                self.phase = Phase::Ready;
                actions.push(Action::Event(SessionEventBody::Ready {
                    context_revision: self.context_revision,
                }));
            }
            Some("task_started") if matches!(self.phase, Phase::Starting { .. }) => {
                let Phase::Starting { text } = std::mem::replace(&mut self.phase, Phase::Speaking)
                else {
                    unreachable!()
                };
                let command_id = self.confirmed_command()?;
                actions.extend([
                    send(json!({"event":"task_continue","text":text})),
                    ack(command_id),
                    Action::Event(SessionEventBody::ResponseStarted {
                        generation_id: self
                            .generation
                            .clone()
                            .ok_or_else(|| protocol("missing generation"))?,
                        input_position: self.input_position,
                    }),
                ]);
            }
            Some("task_canceled") if matches!(self.phase, Phase::Interrupting { .. }) => {
                let Phase::Interrupting { epoch } =
                    std::mem::replace(&mut self.phase, Phase::Speaking)
                else {
                    unreachable!()
                };
                // Until this remote barrier, all in-flight audio keeps its old epoch.
                let id = self.confirmed_command()?;
                self.audio.cancel(epoch)?;
                actions.push(ack(id));
            }
            Some("task_flushed") if matches!(self.phase, Phase::Flushing) => {
                let id = self.confirmed_command()?;
                self.phase = Phase::Speaking;
                actions.push(ack(id));
            }
            Some("task_finished") if matches!(self.phase, Phase::Finishing) => {
                let id = self.confirmed_command()?;
                self.phase = Phase::Finished;
                self.audio.finish()?;
                actions.extend([
                    ack(id),
                    Action::Event(SessionEventBody::ResponseFinished {
                        input_position: self.input_position,
                        generation_id: self
                            .generation
                            .clone()
                            .ok_or_else(|| protocol("missing generation"))?,
                        response_status: ResponseStatus::Completed,
                        model_id: Some(self.config.model.clone()),
                        response_id: value
                            .get("session_id")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        finish_reason: Some("task_finished".into()),
                        usage: Default::default(),
                        provider_data: json!({"extra_info":self.metadata}),
                        effective: Default::default(),
                    }),
                ]);
            }
            Some("sentence_start")
                if matches!(
                    self.phase,
                    Phase::Speaking
                        | Phase::Interrupting { .. }
                        | Phase::Flushing
                        | Phase::Finishing
                ) =>
            {
                self.audio.start()?
            }
            Some("sentence_end")
                if matches!(
                    self.phase,
                    Phase::Speaking
                        | Phase::Interrupting { .. }
                        | Phase::Flushing
                        | Phase::Finishing
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
                        Phase::Speaking
                            | Phase::Interrupting { .. }
                            | Phase::Flushing
                            | Phase::Finishing
                    ) => {}
            None if audio.is_some()
                && matches!(
                    self.phase,
                    Phase::Speaking
                        | Phase::Interrupting { .. }
                        | Phase::Flushing
                        | Phase::Finishing
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
    fn validate_audio_metadata(&self, value: &Value) -> Result<()> {
        for (key, expected) in [
            ("audio_format", json!(self.config.audio.format)),
            ("audio_sample_rate", json!(self.config.audio.sample_rate)),
            ("audio_channel", json!(self.config.audio.channels)),
        ] {
            if value.get(key).is_some_and(|actual| *actual != expected) {
                return Err(protocol(&format!(
                    "provider {key} differs from requested audio settings"
                )));
            }
        }
        Ok(())
    }
    fn confirmed_command(&mut self) -> Result<String> {
        self.confirmation
            .complete()?
            .ok_or_else(|| protocol("missing command confirmation"))
    }
    fn end_sentence(&mut self, actions: &mut Vec<Action>) -> Result<()> {
        actions.push(Action::Media(self.audio.end(&self.session_id)?));
        Ok(())
    }
}
fn send(value: Value) -> Action {
    Action::Send(WireMessage::Text(value.to_string()))
}
fn ack(command_id: String) -> Action {
    acknowledge(command_id, Acknowledgement::Provider)
}
fn acknowledge(command_id: String, level: Acknowledgement) -> Action {
    Action::Event(SessionEventBody::Acknowledged {
        command_id,
        recovery: None,
        level,
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
    if text.is_empty() || text.chars().count() > 10000 {
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
            Capability::FlushInput,
            Capability::ExplicitGeneration,
            Capability::ResponseEvents,
            Capability::StreamingInput,
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
