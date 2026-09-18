//! Live session semantics. No sockets, media queues or executor loop live here.
use super::{adapter::Adapter, codec};
use serde_json::{Value, json};
use zhir_core::{
    Result,
    error::{Error, Failure},
    message::{Message, Output},
    model::*,
    operation::DelegationRequest,
};
use zhir_models::Confirmation;
use zhir_models::webrtc::{Action, WebRtcAdapter, WebRtcProtocol};

enum Phase {
    Idle,
    Starting,
    Active,
    Draining { reason: String, usage: Value },
    Finished,
    Closed,
}
#[derive(Debug)]
enum Pending {
    Start,
    Audio { id: String, enabled: bool },
    Close(String),
}
pub(super) struct Protocol {
    adapter: Adapter,
    phase: Phase,
    request: ModelRequest,
    context_revision: u64,
    confirmation: Confirmation<Pending>,
    end_id: Option<String>,
    close_id: Option<String>,
    remote_session: Value,
    delegations: std::collections::BTreeSet<String>,
    max_delegations: usize,
}
impl Protocol {
    pub fn new(
        adapter: Adapter,
        max_delegations: usize,
        request: ModelRequest,
        context_revision: u64,
    ) -> Self {
        Self {
            adapter,
            max_delegations,
            phase: Phase::Idle,
            request,
            context_revision,
            confirmation: Confirmation::default(),
            end_id: None,
            close_id: None,
            remote_session: Value::Null,
            delegations: Default::default(),
        }
    }
    fn require_delegation(&self, id: &str) -> Result<()> {
        if !self.delegations.contains(id) {
            return Err(Error::Protocol(
                "unknown or completed Live delegation".into(),
            ));
        }
        Ok(())
    }
    fn close_remote(&mut self) -> Result<Action> {
        let id = self
            .close_id
            .as_ref()
            .or(self.end_id.as_ref())
            .cloned()
            .ok_or_else(|| Error::Protocol("Live close has no command identity".into()))?;
        self.confirmation.begin(
            Pending::Close(id),
            "session.closed",
            self.adapter.config.command_timeout,
        )?;
        Ok(Action::Send(json!({"type":"session.close"}).to_string()))
    }
    fn require_output_phase(&self) -> Result<()> {
        if !matches!(self.phase, Phase::Active | Phase::Draining { .. }) {
            return Err(Error::Protocol(
                "Live output outside an active session".into(),
            ));
        }
        Ok(())
    }
    fn observation(&self, data: Value) -> Action {
        emit_event(SessionEventBody::Delta {
            generation_id: None,
            delta: ModelDelta::ProtocolEvent {
                output_index: 0,
                data,
            },
        })
    }
}
impl WebRtcProtocol for Protocol {
    fn initial(&mut self) -> Result<Vec<Action>> {
        self.adapter.negotiate(&self.request)?;
        let config = &self.adapter.config;
        let session = json!({"model":config.model,"instructions":config.instructions,
            "audio":{"output":{"voice":config.voice}},
            "delegation":{"type":"client","ack_filler":false},
            "initial_items":codec::initial_items(&self.request)?});
        self.confirmation
            .begin(Pending::Start, "session.started", config.connect_timeout)?;
        self.phase = Phase::Starting;
        Ok(vec![Action::Connect {
            payload: session,
            deadline: self
                .confirmation
                .deadline()
                .ok_or_else(|| Error::Protocol("missing Live startup deadline".into()))?,
            timeout: Error::Uncertain("Live session startup timed out".into()),
        }])
    }
    fn deadline(&self) -> Option<tokio::time::Instant> {
        self.confirmation.deadline()
    }
    fn check_deadline(&self) -> Result<()> {
        self.confirmation.check()
    }
    fn commands_allowed(&self) -> bool {
        matches!(self.phase, Phase::Idle | Phase::Active | Phase::Finished)
            && self.confirmation.pending().is_none()
    }
    fn input_allowed(&self) -> bool {
        matches!(self.phase, Phase::Active) && self.end_id.is_none() && self.close_id.is_none()
    }
    fn draining(&self) -> bool {
        matches!(self.phase, Phase::Draining { .. })
    }
    fn finished(&self) -> bool {
        matches!(self.phase, Phase::Finished | Phase::Closed)
    }
    fn closed(&self) -> bool {
        matches!(self.phase, Phase::Closed)
    }

    fn command(&mut self, command: SessionCommand) -> Result<Vec<Action>> {
        if command.id.is_empty() || !self.commands_allowed() {
            return Err(Error::Invalid(
                "invalid Live command identity or order".into(),
            ));
        }
        match command.body {
            SessionCommandBody::Append {
                source: AppendSource::Accepted,
                ..
            } => Ok(vec![acknowledge(command.id, Acknowledgement::Projection)]),
            SessionCommandBody::Append { entry, .. }
                if matches!(
                    entry.message,
                    Message::User { .. } | Message::External { .. }
                ) && self.input_allowed() =>
            {
                let mut actions = append(
                    "session.context.append",
                    &command.id,
                    &codec::text(&entry.message)?,
                    None,
                    "commentary",
                );
                // This endpoint does not echo a correlating command identity.
                // The acknowledgement follows transport acceptance of every fragment.
                actions.push(ack(command.id));
                Ok(actions)
            }
            SessionCommandBody::DelegationContext {
                origin, content, ..
            } if matches!(self.phase, Phase::Active) => {
                self.require_delegation(&origin.call_id)?;
                let mut actions = append(
                    "delegation.context.append",
                    &command.id,
                    &codec::context_text(content)?,
                    Some(&origin.call_id),
                    "commentary",
                );
                actions.push(ack(command.id));
                Ok(actions)
            }
            SessionCommandBody::Append {
                entry:
                    zhir_core::run::HistoryEntry {
                        message: Message::DelegationResult { id, outcome },
                        ..
                    },
                ..
            } if matches!(self.phase, Phase::Active) => {
                self.require_delegation(&id)?;
                let mut actions = append(
                    "delegation.context.append",
                    &command.id,
                    &codec::outcome_text(outcome)?,
                    Some(&id),
                    "speakable",
                );
                self.delegations.remove(&id);
                actions.push(ack(command.id));
                Ok(actions)
            }
            SessionCommandBody::SetInputAudio { enabled } if self.input_allowed() => {
                self.confirmation.begin(
                    Pending::Audio {
                        id: command.id.clone(),
                        enabled,
                    },
                    if enabled {
                        "input_audio.resumed"
                    } else {
                        "input_audio.paused"
                    },
                    self.adapter.config.command_timeout,
                )?;
                Ok(vec![Action::Send(
                    json!({"type":if enabled {"input_audio.resume"} else {"input_audio.pause"},"event_id":command.id}).to_string(),
                )])
            }
            SessionCommandBody::SealUserInput if self.input_allowed() => {
                self.end_id = Some(command.id.clone());
                Ok(vec![Action::DrainInput, ack(command.id)])
            }
            SessionCommandBody::Close if matches!(self.phase, Phase::Finished) => {
                self.phase = Phase::Closed;
                Ok(vec![ack(command.id)])
            }
            SessionCommandBody::Close if matches!(self.phase, Phase::Active) => {
                self.close_id = Some(command.id);
                Ok(vec![self.close_remote()?])
            }
            _ => Err(Error::Invalid(
                "unsupported Live command or command order".into(),
            )),
        }
    }
    fn receive(&mut self, text: &str) -> Result<Vec<Action>> {
        self.confirmation.check()?;
        let value: Value = serde_json::from_str(text)
            .map_err(|_| Error::Protocol("invalid Live event JSON".into()))?;
        let event: codec::ServerEvent = serde_json::from_value(value.clone())
            .map_err(|_| Error::Protocol("invalid Live event fields".into()))?;
        match event {
            codec::ServerEvent::Started { session } => {
                if !matches!(self.phase, Phase::Starting) {
                    return Err(Error::Protocol("unexpected or duplicate Live start".into()));
                }
                if session
                    .get("id")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                {
                    return Err(Error::Protocol(
                        "Live start lacks a remote session identity".into(),
                    ));
                }
                let Pending::Start = self.confirmation.complete()? else {
                    return Err(Error::Protocol("unexpected Live start confirmation".into()));
                };
                self.remote_session = session;
                self.phase = Phase::Active;
                Ok(vec![emit_event(SessionEventBody::Ready {
                    context_revision: self.context_revision,
                })])
            }
            codec::ServerEvent::AudioPaused | codec::ServerEvent::AudioResumed => {
                let enabled = matches!(event, codec::ServerEvent::AudioResumed);
                if !matches!(self.confirmation.pending(), Some(Pending::Audio { enabled: expected, .. }) if *expected == enabled)
                {
                    return Err(Error::Protocol(
                        "Live audio input acknowledgement mismatch".into(),
                    ));
                }
                let Pending::Audio { id, .. } = self.confirmation.complete()? else {
                    unreachable!()
                };
                Ok(vec![acknowledge(id, Acknowledgement::Provider)])
            }
            codec::ServerEvent::Turn { turn } => {
                self.require_output_phase()?;
                if turn.id.is_empty() {
                    return Err(Error::Protocol("empty Live conversation identity".into()));
                }
                let message = match turn.role.as_str() {
                    "user" => Message::user(turn.transcript),
                    "assistant" => Message::Assistant {
                        output: vec![Output::text(turn.transcript)],
                        provider_data: Value::Null,
                    },
                    _ => return Err(Error::Protocol("invalid Live speaker".into())),
                };
                Ok(vec![emit_event(SessionEventBody::ConversationItem {
                    item_id: turn.id,
                    message,
                })])
            }
            codec::ServerEvent::Delegation { item } => {
                self.require_output_phase()?;
                if item.id.is_empty()
                    || item.kind != "delegation"
                    || item.target != "client"
                    || item.content.iter().any(|part| part.kind != "input_text")
                {
                    return Err(Error::Protocol("unsupported Live delegation".into()));
                }
                if !self.delegations.contains(&item.id)
                    && self.delegations.len() >= self.max_delegations
                {
                    return Err(Error::Protocol("Live delegation capacity exceeded".into()));
                }
                if self.draining() && self.close_id.is_none() {
                    return Err(Error::Uncertain(
                        "Live closed with an unfinished delegation".into(),
                    ));
                }
                self.delegations.insert(item.id.clone());
                Ok(vec![emit_event(SessionEventBody::Output {
                    generation_id: None,
                    item_id: item.id.clone(),
                    caller_id: "live".into(),
                    output: Output::Delegation {
                        request: DelegationRequest {
                            id: item.id,
                            prompt: item.content.into_iter().map(|part| part.text).collect(),
                        },
                    },
                })])
            }
            codec::ServerEvent::Closed { reason, usage } => {
                if self.finished() || self.draining() {
                    return Err(Error::Protocol("duplicate Live closure".into()));
                }
                if reason == "connection_lost"
                    || (!self.delegations.is_empty() && self.close_id.is_none())
                    || (matches!(self.confirmation.pending(), Some(Pending::Close(_)))
                        && reason != "client_request")
                {
                    return Err(Error::Uncertain(format!(
                        "Live closed without confirming requested completion: {reason}"
                    )));
                }
                match self.confirmation.pending() {
                    Some(Pending::Close(id))
                        if self.close_id.as_ref().or(self.end_id.as_ref()) == Some(id) =>
                    {
                        self.confirmation.complete()?;
                    }
                    None if matches!(self.phase, Phase::Active) => (),
                    _ => {
                        return Err(Error::Uncertain(
                            "Live closed before pending command confirmation".into(),
                        ));
                    }
                }
                self.phase = Phase::Draining { reason, usage };
                Ok(vec![Action::Disconnect])
            }
            codec::ServerEvent::Error { error } => Ok(vec![
                self.observation(value),
                Action::Fail(Error::Model(Failure::new(error.code, error.message))),
            ]),
            codec::ServerEvent::Observation => Ok(vec![self.observation(value)]),
        }
    }
    fn finalize(&mut self) -> Result<Vec<Action>> {
        let next = if self.close_id.is_some() {
            Phase::Closed
        } else {
            Phase::Finished
        };
        let Phase::Draining { reason, usage } = std::mem::replace(&mut self.phase, next) else {
            return Err(Error::Protocol(
                "Live finalized before remote closure".into(),
            ));
        };
        let mut actions = vec![];
        self.end_id.take();
        if let Some(id) = self.close_id.take() {
            actions.push(acknowledge(id, Acknowledgement::Provider));
        }
        actions.push(emit_event(SessionEventBody::Closed {
            reason,
            provider_data: json!({"session":self.remote_session,"session_usage":usage}),
        }));
        Ok(actions)
    }
}

fn ack(command_id: String) -> Action {
    acknowledge(command_id, Acknowledgement::Transport)
}
fn acknowledge(command_id: String, level: Acknowledgement) -> Action {
    emit_event(SessionEventBody::Acknowledged {
        command_id,
        recovery: None,
        level,
    })
}
fn append(
    kind: &str,
    id: &str,
    text: &str,
    delegation: Option<&str>,
    channel: &str,
) -> Vec<Action> {
    codec::context(kind, id, text, delegation)
        .into_iter()
        .map(|mut value| {
            value["channel"] = json!(channel);
            Action::Send(value.to_string())
        })
        .collect()
}

fn emit_event(body: SessionEventBody) -> Action {
    Action::Event(Box::new(body))
}
