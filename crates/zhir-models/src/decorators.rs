use std::sync::Arc;
use zhir_core::{
    BoxFuture, Result, error::Error, model::*, profile::NegotiatedProfile, run::RunContext,
};
type ObserverFactory =
    dyn Fn(&ModelRequest, &RunContext) -> Result<Arc<dyn DeltaSink>> + Send + Sync;
pub struct ObservedModel {
    inner: Arc<dyn Model>,
    factory: Arc<ObserverFactory>,
}
impl ObservedModel {
    pub fn new(
        inner: Arc<dyn Model>,
        factory: impl Fn(&ModelRequest, &RunContext) -> Result<Arc<dyn DeltaSink>>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            inner,
            factory: Arc::new(factory),
        }
    }
}
struct ObservedEvents {
    inner: Box<dyn SessionReceiver>,
    observer: Arc<dyn DeltaSink>,
}
impl SessionReceiver for ObservedEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            let event = self.inner.receive().await?;
            if let Some(SessionEvent {
                body: SessionEventBody::Delta { delta, .. },
                ..
            }) = &event
            {
                self.observer.emit(delta.clone()).await?;
            }
            Ok(event)
        })
    }
}
impl Model for ObservedModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            open.context.cancellation.check()?;
            let observer = (self.factory)(&open.request, &open.context.run)?;
            let mut session = self.inner.open_session(open).await?;
            session.output = Box::new(ObservedEvents {
                inner: session.output,
                observer,
            });
            Ok(session)
        })
    }
}
/// Retries session establishment only. A sent command is never transparently replayed.
pub struct RetryingModel {
    inner: Arc<dyn Model>,
    policy: zhir_policies::RetryPolicy,
}
impl RetryingModel {
    pub fn new(inner: Arc<dyn Model>, policy: zhir_policies::RetryPolicy) -> Result<Self> {
        Ok(Self { inner, policy })
    }
}
impl Model for RetryingModel {
    fn capabilities(&self) -> &CapabilitySet {
        self.inner.capabilities()
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.inner.negotiate(request)
    }
    fn open_session(&self, open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            let deadline = zhir_policies::timing::deadline(&open.context.run)?;
            for attempt in 0..self.policy.max_attempts() {
                zhir_policies::timing::check(&open.context.cancellation, deadline)?;
                match self.inner.open_session(open.clone()).await {
                    Err(Error::Model(error))
                        if error.retryable
                            && open.recovery.is_none()
                            && attempt + 1 < self.policy.max_attempts() =>
                    {
                        zhir_policies::timing::wait(
                            self.policy
                                .delay_after(attempt + 1)
                                .expect("remaining attempt"),
                            &open.context.cancellation,
                            deadline,
                            |at| tokio::time::sleep_until(at.into()),
                        )
                        .await?
                    }
                    result => return result,
                }
            }
            unreachable!("positive retry attempts")
        })
    }
}
/// A stable adapter identity used to bind recovery even when candidate order changes.
pub struct FallbackCandidate {
    pub id: String,
    pub model: Arc<dyn Model>,
}
impl FallbackCandidate {
    pub fn new(id: impl Into<String>, model: Arc<dyn Model>) -> Self {
        Self {
            id: id.into(),
            model,
        }
    }
}
/// Candidates negotiate independently. Once opened, commands and recovery remain
/// bound to that candidate; failures after dispatch never select another model.
pub struct FallbackModel {
    models: Vec<FallbackCandidate>,
    capabilities: CapabilitySet,
}
impl FallbackModel {
    pub fn new(models: Vec<FallbackCandidate>) -> Result<Self> {
        let mut identities = std::collections::BTreeSet::new();
        if models
            .iter()
            .any(|candidate| candidate.id.is_empty() || !identities.insert(&candidate.id))
        {
            return Err(Error::Invalid(
                "fallback candidates require unique nonempty identities".into(),
            ));
        }
        let mut capabilities = models
            .first()
            .ok_or_else(|| Error::Invalid("fallback requires models".into()))?
            .model
            .capabilities()
            .clone();
        for candidate in models.iter().skip(1) {
            let caps = candidate.model.capabilities();
            capabilities.features.extend(caps.features.iter().copied());
            for (values, added) in [
                (&mut capabilities.input_modalities, &caps.input_modalities),
                (&mut capabilities.output_modalities, &caps.output_modalities),
                (&mut capabilities.tool_choices, &caps.tool_choices),
            ] {
                for value in added {
                    if !values.contains(value) {
                        values.push(value.clone());
                    }
                }
            }
            for (key, values) in &caps.constraints {
                let supported = capabilities.constraints.entry(key.clone()).or_default();
                for value in values {
                    if !supported.contains(value) {
                        supported.push(value.clone());
                    }
                }
            }
        }
        Ok(Self {
            models,
            capabilities,
        })
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct FallbackRecovery {
    candidate: String,
    reference: zhir_core::operation::RecoveryRef,
}
struct BoundEvents {
    candidate: String,
    inner: Box<dyn SessionReceiver>,
}
impl SessionReceiver for BoundEvents {
    fn receive(&mut self) -> BoxFuture<'_, Result<Option<SessionEvent>>> {
        Box::pin(async move {
            let Some(mut event) = self.inner.receive().await? else {
                return Ok(None);
            };
            let reference = match &mut event.body {
                SessionEventBody::Acknowledged {
                    recovery: Some(reference),
                    ..
                }
                | SessionEventBody::Recovery { reference } => Some(reference),
                _ => None,
            };
            if let Some(reference) = reference {
                *reference = zhir_core::operation::RecoveryRef {
                    adapter: "zhir.fallback".into(),
                    data: serde_json::to_value(FallbackRecovery {
                        candidate: self.candidate.clone(),
                        reference: reference.clone(),
                    })
                    .map_err(|error| Error::Protocol(error.to_string()))?,
                };
            }
            Ok(Some(event))
        })
    }
}
fn bind(mut session: ModelSession, candidate: &str) -> ModelSession {
    session.output = Box::new(BoundEvents {
        candidate: candidate.into(),
        inner: session.output,
    });
    session
}
impl Model for FallbackModel {
    fn capabilities(&self) -> &CapabilitySet {
        &self.capabilities
    }
    fn negotiate(&self, request: &ModelRequest) -> Result<NegotiatedProfile> {
        self.models
            .iter()
            .find_map(|candidate| candidate.model.negotiate(request).ok())
            .ok_or_else(|| Error::Invalid("no model satisfies request".into()))
    }
    fn open_session(&self, mut open: SessionOpen) -> BoxFuture<'_, Result<ModelSession>> {
        Box::pin(async move {
            open.context.cancellation.check()?;
            if let Some(reference) = open.recovery.take() {
                if reference.adapter != "zhir.fallback" {
                    return Err(Error::Invalid(
                        "recovery does not belong to this fallback adapter".into(),
                    ));
                }
                let saved: FallbackRecovery = serde_json::from_value(reference.data)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let candidate = self
                    .models
                    .iter()
                    .find(|candidate| candidate.id == saved.candidate)
                    .ok_or_else(|| {
                        Error::Invalid("original fallback candidate is unavailable".into())
                    })?;
                open.recovery = Some(saved.reference);
                return candidate
                    .model
                    .open_session(open)
                    .await
                    .map(|session| bind(session, &candidate.id));
            }
            let mut error = Error::Invalid("no model satisfies request".into());
            for candidate in &self.models {
                open.context.cancellation.check()?;
                if candidate.model.negotiate(&open.request).is_err() {
                    continue;
                }
                match candidate.model.open_session(open.clone()).await {
                    Ok(session) => return Ok(bind(session, &candidate.id)),
                    Err(Error::Model(e)) if e.retryable => error = Error::Model(e),
                    Err(e) => return Err(e),
                }
            }
            Err(error)
        })
    }
}
