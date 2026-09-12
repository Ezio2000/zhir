//! Offline verification of durable checkpoint traces; never repeats effects.
use std::sync::Arc;
use zhir_core::{
    Result,
    error::Error,
    message::{Message, Output, visible_content},
    run::{ActiveState, Checkpoint, Fact, Metrics, State},
};
fn require(valid: bool, message: &str) -> Result<()> {
    if valid {
        Ok(())
    } else {
        Err(Error::Protocol(message.into()))
    }
}
fn active(state: &State) -> Option<ActiveState> {
    match state {
        State::Suspended { resume_to, .. } => Some(resume_to.clone()),
        _ => state.active(),
    }
}
fn idle(state: &State) -> bool {
    matches!(
        state,
        State::Planning {
            provider_turn_pending: false
        }
    )
}
fn metrics(before: &Checkpoint, after: &Checkpoint, steps: u64, calls: u64) -> Result<()> {
    require(
        before.metrics.planning_steps.checked_add(steps) == Some(after.metrics.planning_steps)
            && before.metrics.runtime_tool_calls.checked_add(calls)
                == Some(after.metrics.runtime_tool_calls),
        "fact and metrics disagree",
    )?;
    if steps == 0 {
        require(
            before.metrics.usage == after.metrics.usage,
            "usage changed without a model turn",
        )?;
    } else {
        let a = &before.metrics.usage;
        let b = &after.metrics.usage;
        for (old, new) in [
            (a.input_tokens, b.input_tokens),
            (a.output_tokens, b.output_tokens),
            (a.total_tokens, b.total_tokens),
            (a.reasoning_tokens, b.reasoning_tokens),
            (a.cache_read_tokens, b.cache_read_tokens),
            (a.cache_write_tokens, b.cache_write_tokens),
        ] {
            require(
                old.is_none_or(|old| new.is_some_and(|new| new >= old)),
                "usage regressed",
            )?;
        }
    }
    Ok(())
}
/// Verify a contiguous trace, including the history changes described by each fact.
/// A trace may begin at a recovered checkpoint; earlier revisions need not be supplied.
pub fn verify_trace(checkpoints: &[Arc<Checkpoint>]) -> Result<()> {
    let first = checkpoints
        .first()
        .ok_or_else(|| Error::Invalid("empty trace".into()))?;
    first.validate()?;
    if first.revision == 0 {
        require(
            matches!(first.fact, Fact::Started)
                && idle(&first.state)
                && first.metrics == Metrics::default(),
            "invalid initial checkpoint",
        )?;
    }
    for pair in checkpoints.windows(2) {
        let (before, after) = (&pair[0], &pair[1]);
        after.validate()?;
        require(
            before.options == after.options,
            "run options changed after start",
        )?;
        require(
            !before.state.terminal()
                && after.id != before.id
                && after.context.run_id == before.context.run_id
                && before.revision.checked_add(1) == Some(after.revision)
                && after.parent_id.as_ref() == Some(&before.id),
            "invalid trace transition",
        )?;
        let mut context = after.context.clone();
        if matches!(after.fact, Fact::Resumed) {
            context.metadata = before.context.metadata.clone();
        }
        require(
            context == before.context,
            "run context changed outside resume metadata",
        )?;
        let previous = before.history.messages();
        let current = after.history.messages();
        let appended = if !matches!(after.fact, Fact::HistoryRewrite { .. }) {
            require(
                current.starts_with(&previous),
                "history prefix changed without rewrite",
            )?;
            &current[previous.len()..]
        } else {
            &[]
        };
        match &after.fact {
            Fact::Started => {
                return Err(Error::Protocol(
                    "started fact after initial checkpoint".into(),
                ));
            }
            Fact::Resumed => {
                let State::Suspended { resume_to, .. } = &before.state else {
                    return Err(Error::Protocol("resume without suspension".into()));
                };
                require(
                    after.state == resume_to.clone().into_state()
                        && (appended.is_empty() || idle(&after.state)),
                    "invalid resume target or messages",
                )?;
                metrics(before, after, 0, 0)?;
            }
            Fact::ModelTurn {
                runtime_tool_call_ids,
                result,
            } => {
                require(
                    matches!(before.state, State::Planning { .. })
                        && appended.len() == 1
                        && result == after.state.kind(),
                    "invalid model turn transition",
                )?;
                let Message::Assistant { output, .. } = &appended[0] else {
                    return Err(Error::Protocol(
                        "model turn must append assistant output".into(),
                    ));
                };
                let ids = output
                    .iter()
                    .filter_map(|o| match o {
                        Output::RuntimeToolCall { call } => Some(call.id.clone()),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                require(
                    &ids == runtime_tool_call_ids,
                    "model fact differs from output calls",
                )?;
                require(
                    matches!(
                        after.state,
                        State::Planning { .. }
                            | State::RuntimeToolsPending { .. }
                            | State::Completed { .. }
                            | State::Limited { .. }
                    ),
                    "invalid model result state",
                )?;
                if let State::Completed { content } = &after.state {
                    require(
                        content == &visible_content(output) && ids.is_empty(),
                        "completed content differs from model output",
                    )?;
                }
                metrics(before, after, 1, 0)?;
            }
            Fact::RuntimeToolBatch {
                call_ids,
                outcomes,
                parallel,
            } => {
                let State::RuntimeToolsPending {
                    calls,
                    provider_turn_pending,
                } = &before.state
                else {
                    return Err(Error::Protocol("tool batch without pending calls".into()));
                };
                require(
                    !call_ids.is_empty()
                        && call_ids.len() <= calls.len()
                        && call_ids.len() == outcomes.len()
                        && appended.len() == call_ids.len()
                        && (*parallel || call_ids.len() == 1),
                    "invalid tool batch size",
                )?;
                for (index, message) in appended.iter().enumerate() {
                    let Message::RuntimeTool {
                        call_id,
                        name,
                        outcome,
                    } = message
                    else {
                        return Err(Error::Protocol(
                            "tool batch appended a non-tool message".into(),
                        ));
                    };
                    require(
                        call_id == &calls[index].id
                            && name == &calls[index].name
                            && call_id == &call_ids[index]
                            && outcomes[index] == outcome.kind(),
                        "tool fact differs from ordered results",
                    )?;
                }
                let rest = calls[call_ids.len()..].to_vec();
                let expected = if rest.is_empty() {
                    ActiveState::Planning {
                        provider_turn_pending: *provider_turn_pending,
                    }
                } else {
                    ActiveState::RuntimeToolsPending {
                        calls: rest,
                        provider_turn_pending: *provider_turn_pending,
                    }
                };
                require(
                    active(&after.state) == Some(expected),
                    "tool batch has invalid remaining work",
                )?;
                metrics(before, after, 0, call_ids.len() as u64)?;
            }
            Fact::ConversationInsert { .. } => {
                require(
                    idle(&before.state) && idle(&after.state) && !appended.is_empty(),
                    "insert outside idle planning",
                )?;
                metrics(before, after, 0, 0)?;
            }
            Fact::HistoryRewrite { .. } => {
                require(
                    idle(&before.state) && idle(&after.state),
                    "rewrite outside idle planning",
                )?;
                metrics(before, after, 0, 0)?;
            }
            Fact::Control { action } => {
                require(appended.is_empty(), "control changed history")?;
                let valid = match (action.as_str(), &after.state) {
                    ("failed", State::Failed { .. }) | ("limited", State::Limited { .. }) => true,
                    ("suspended", State::Suspended { resume_to, .. }) => {
                        before.state.active().as_ref() == Some(resume_to)
                    }
                    _ => false,
                };
                require(valid, "control fact and state disagree")?;
                metrics(before, after, 0, 0)?;
            }
        }
    }
    Ok(())
}
