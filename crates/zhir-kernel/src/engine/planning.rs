use super::*;

impl Engine {
    pub(super) async fn planning(&mut self, provider_pending: bool) -> Result<()> {
        if self.current().metrics.planning_steps >= self.options.limits.max_planning_steps {
            return self.limit(LimitReason::PlanningSteps).await;
        }
        if !provider_pending && let Some(reducer) = self.config.history_reducer.clone() {
            let checkpoint = self.current();
            let future = Box::pin(async move { reducer.reduce(checkpoint).await });
            let effect = self
                .effect(future, Cancellation::default(), true, false)
                .await;
            let Some(rewrite) = self.interruption(effect).await? else {
                return Ok(());
            };
            if let Some(rewrite) = rewrite {
                let history = History::new(rewrite.messages.clone())?;
                validate_history(
                    &history,
                    Some(&ActiveState::Planning {
                        provider_turn_pending: false,
                    }),
                )?;
                self.advance(
                    State::Planning {
                        provider_turn_pending: false,
                    },
                    Fact::HistoryRewrite {
                        reason: rewrite.reason,
                    },
                    HistoryDelta::Replace(rewrite.messages),
                    None,
                )
                .await?;
            }
        }
        let checkpoint = self.current();
        let model = self.config.model.clone();
        let request = ModelRequest {
            messages: checkpoint.history.messages(),
            runtime_tools: self.catalog.as_ref().expect("opened catalog").specs(),
            provider_tools: self.options.provider_tools.clone(),
            options: self.options.model.clone(),
            tool_choice: self.options.tool_choice.clone(),
            response_format: self.options.response_format.clone(),
            stream: self.options.stream,
        };
        request.validate(model.capabilities())?;
        let cancellation = Cancellation::default();
        let context = ModelContext {
            run: checkpoint.context.clone(),
            cancellation: cancellation.clone(),
            deltas: if self.options.stream {
                Some(Arc::new(self.emitter.clone()))
            } else {
                None
            },
        };
        self.emitter.emit(EventData::ModelStarted);
        let effect = self
            .effect(
                Box::pin(async move { model.invoke(request, context).await }),
                cancellation,
                true,
                provider_pending,
            )
            .await;
        let Some(response) = self.interruption(effect).await? else {
            return Ok(());
        };
        if self.expired() {
            return self.limit(LimitReason::Deadline).await;
        }
        response.validate()?;
        self.emitter.emit(EventData::ModelFinished);
        let mut metrics = checkpoint.metrics.clone();
        metrics.planning_steps += 1;
        metrics.usage.add(&response.usage);
        let calls = response
            .output
            .iter()
            .filter_map(|o| {
                if let Output::RuntimeToolCall { call } = o {
                    Some(call.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        let state = if self
            .options
            .limits
            .max_total_tokens
            .is_some_and(|limit| metrics.usage.total_tokens.is_some_and(|n| n >= limit))
        {
            State::Limited {
                reason: LimitReason::TotalTokens,
            }
        } else if !calls.is_empty() {
            State::RuntimeToolsPending {
                calls: PendingCalls {
                    message_index: checkpoint.history.len(),
                    next: 0,
                    end: calls.len(),
                },
                provider_turn_pending: response.provider_turn_pending,
            }
        } else if response.provider_turn_pending || !self.inserts.is_empty() {
            State::Planning {
                provider_turn_pending: response.provider_turn_pending,
            }
        } else {
            State::Completed {
                content: visible_content(&response.output),
            }
        };
        let fact = Fact::ModelTurn {
            runtime_tool_call_ids: calls.iter().map(|c| c.id.clone()).collect(),
            result: state.kind(),
        };
        self.advance(
            state,
            fact,
            HistoryDelta::Append(vec![Message::Assistant {
                output: response.output,
                provider_data: response.provider_data,
            }]),
            Some(metrics),
        )
        .await
    }
}
