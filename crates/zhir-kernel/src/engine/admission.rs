use super::*;

impl Engine {
    pub(super) fn call(&self, operation: &OperationRecord) -> Result<RuntimeToolCall> {
        let entry = self
            .current
            .history
            .get(operation.call_entry)
            .ok_or_else(|| Error::Protocol("missing operation call".into()))?;
        if let Message::Assistant { output, .. } = &entry.message {
            for item in output {
                if let Output::RuntimeToolCall { call } = item
                    && call.id == operation.origin.call_id
                {
                    return Ok(call.clone());
                }
            }
        }
        Err(Error::Protocol("operation call entry mismatch".into()))
    }
    pub(super) fn tool_context(
        &self,
        id: String,
        cancellation: Cancellation,
    ) -> RuntimeToolContext {
        RuntimeToolContext {
            run: self.current.context.clone(),
            operation_id: id.clone(),
            cancellation,
            progress: Some(Arc::new(ToolProgress {
                operation_id: id.clone(),
                sender: self.work_tx.clone(),
            })),
        }
    }
    pub(super) fn spawn_tool(
        &mut self,
        id: String,
        binding: Arc<dyn RuntimeToolBinding>,
        recovery: Option<OperationRecord>,
    ) -> Result<()> {
        self.check()?;
        self.pending_starts.insert(id.clone());
        let token = Cancellation::default();
        let context = self.tool_context(id.clone(), token.clone());
        self.tokens.insert(id.clone(), token);
        let tx = self.work_tx.clone();
        self.tasks.spawn(async move {
            let result = match recovery {
                Some(record) => binding
                    .recover(record, context)
                    .await
                    .map_err(|error| Error::Protocol(format!("recovery failed: {error}"))),
                None => binding.start(context).await,
            };
            let _ = tx.send(Work::Started(id, result)).await;
        });
        Ok(())
    }
    pub(super) async fn admit(&mut self) -> Result<()> {
        let running = self
            .current
            .active
            .operations
            .values()
            .filter(|o| {
                matches!(
                    o.state,
                    OperationState::Running | OperationState::Cancelling
                )
            })
            .count();
        if running + self.admitting.len() + self.current.active.commands.len()
            >= self.current.options.limits.max_control_commands
            || running + self.admitting.len()
                >= self.current.options.limits.max_runtime_tool_concurrency
        {
            return Ok(());
        }
        let records: Vec<_> = self
            .current
            .active
            .operations
            .values()
            .filter(|o| o.state == OperationState::Queued && !self.admitting.contains(&o.id))
            .cloned()
            .collect();
        let mut candidates = vec![];
        let mut ids = BTreeMap::new();
        for record in records {
            if matches!(record.owner, OperationOwner::RuntimeTool { .. }) {
                let call = self.call(&record)?;
                let mut candidate = call.clone();
                candidate.id = record.id.clone();
                ids.insert(record.id, call);
                candidates.push(candidate);
            }
        }
        if candidates.is_empty() {
            return Ok(());
        }
        let specs = self
            .catalog
            .specs()
            .into_iter()
            .map(|s| (s.name.clone(), s))
            .collect();
        let admission = self.config.scheduler.select(&candidates, &specs)?;
        let mut bindings = vec![];
        let mut requests = vec![];
        let mut selected = BTreeSet::new();
        for call in admission.calls.into_iter().take(
            self.current.options.limits.max_runtime_tool_concurrency
                - running
                - self.admitting.len(),
        ) {
            if !candidates.contains(&call) || !selected.insert(call.id.clone()) {
                return Err(Error::Invalid(
                    "scheduler returned duplicate or unknown call".into(),
                ));
            }
            let id = call.id.clone();
            let call = ids[&id].clone();
            let binding = match self.catalog.bind(&call) {
                Ok(binding) => binding,
                Err(error) => {
                    self.finish(
                        &id,
                        RuntimeToolOutcome::Failure {
                            error: crate::failure::failure(&error),
                        },
                    )
                    .await?;
                    continue;
                }
            };
            if (running > 0 || !bindings.is_empty())
                && (!admission.parallel || !binding.spec().execution.parallel)
            {
                break;
            }
            if running > 0
                && self
                    .current
                    .active
                    .operations
                    .values()
                    .filter(|o| o.state == OperationState::Running)
                    .any(|o| match &o.owner {
                        OperationOwner::RuntimeTool { name } => !specs
                            .get(name)
                            .is_some_and(|s: &RuntimeToolSpec| s.execution.parallel),
                        _ => false,
                    })
            {
                break;
            }
            requests.push(ApprovalRequest {
                call,
                spec: binding.spec().clone(),
            });
            self.admitting.insert(id.clone());
            bindings.push((id, binding));
        }
        if bindings.is_empty() {
            return Ok(());
        }
        let approval = self.config.approval.clone();
        let context = self.current.context.clone();
        let tx = self.work_tx.clone();
        self.tasks.spawn(async move {
            let result = if let Some(approval) = approval {
                approval.decide(requests, context).await
            } else {
                Ok(vec![ApprovalDecision::Allow; bindings.len()])
            };
            let _ = tx.send(Work::Admitted(bindings, result)).await;
        });
        Ok(())
    }
}

impl Engine {
    pub(super) async fn recover_tools(&mut self) -> Result<()> {
        let recovering: Vec<_> = self
            .current
            .active
            .operations
            .values()
            .filter(|o| {
                matches!(
                    o.state,
                    OperationState::Running | OperationState::Waiting | OperationState::Cancelling
                )
            })
            .cloned()
            .collect();
        for record in recovering {
            if let OperationOwner::RuntimeTool { .. } = &record.owner {
                let call = self.call(&record)?;
                match self.catalog.bind(&call) {
                    Ok(binding) => self.spawn_tool(record.id.clone(), binding, Some(record))?,
                    Err(error) => {
                        self.unknown(&record.id, format!("recovery binding failed: {error}"))
                            .await?
                    }
                }
            }
        }
        Ok(())
    }
}

impl Engine {
    pub(super) async fn admitted(
        &mut self,
        bindings: Vec<(String, Arc<dyn RuntimeToolBinding>)>,
        decisions: Result<Vec<ApprovalDecision>>,
    ) -> Result<()> {
        let decisions = decisions?;
        if decisions.len() != bindings.len() {
            return Err(Error::Protocol(
                "approval decision count differs from requests".into(),
            ));
        }
        for (id, _) in &bindings {
            self.admitting.remove(id);
        }
        if let Some(suspension) = decisions.iter().find_map(|d| match d {
            ApprovalDecision::Suspend(s) => Some(s.clone()),
            _ => None,
        }) {
            let mut next = self.current.as_ref().clone();
            next.state = State::Suspended { suspension };
            return self
                .commit(
                    next,
                    Fact::Control {
                        action: ControlAction::Suspended,
                    },
                    HistoryDelta::Unchanged,
                )
                .await;
        }
        for ((id, binding), decision) in bindings.into_iter().zip(decisions) {
            if self
                .current
                .active
                .operations
                .get(&id)
                .is_none_or(|o| o.state != OperationState::Queued)
            {
                continue;
            }
            match decision {
                ApprovalDecision::Allow => {
                    self.check()?;
                    if self.current.metrics.runtime_tool_calls
                        >= self.current.options.limits.max_runtime_tool_calls
                    {
                        let mut next = self.current.as_ref().clone();
                        next.state = State::Limited {
                            reason: LimitReason::RuntimeToolCalls,
                        };
                        return self
                            .commit(
                                next,
                                Fact::Control {
                                    action: ControlAction::Limited,
                                },
                                HistoryDelta::Unchanged,
                            )
                            .await;
                    }
                    let mut next = self.current.as_ref().clone();
                    next.active.operations.get_mut(&id).expect("queued").state =
                        OperationState::Running;
                    next.metrics.runtime_tool_calls += 1;
                    self.commit(
                        next,
                        Fact::Operation {
                            operation_id: id.clone(),
                            state: OperationState::Running,
                        },
                        HistoryDelta::Unchanged,
                    )
                    .await?;
                    self.spawn_tool(id, binding, None)?;
                }
                ApprovalDecision::Deny(reason) => {
                    self.finish(
                        &id,
                        RuntimeToolOutcome::Failure {
                            error: Failure::new("approval_denied", reason),
                        },
                    )
                    .await?
                }
                ApprovalDecision::Suspend(_) => {
                    unreachable!("suspensions handled before admission")
                }
            }
        }
        Ok(())
    }
}
