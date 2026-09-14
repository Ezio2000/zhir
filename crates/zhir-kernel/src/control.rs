use tokio::sync::{mpsc, oneshot};
use zhir_core::{
    Cancellation, Result, error::Error, message::Message, profile::RequestProfile, run::Suspension,
};
#[derive(Debug, Clone)]
pub struct ControlReceipt {
    pub command_id: String,
    pub revision: u64,
}
#[derive(Debug)]
pub(crate) enum ControlBody {
    Pause(Suspension),
    Input(Message, String),
    CancelOperation(String),
    ReplyOperation(String, serde_json::Value),
    UpdateProfile(RequestProfile),
    InterruptOutput,
    EndInput,
}
pub(crate) struct Control {
    pub body: ControlBody,
    pub reply: oneshot::Sender<Result<ControlReceipt>>,
}
#[derive(Clone)]
pub struct ControlHandle {
    pub(crate) sender: mpsc::Sender<Control>,
    pub(crate) cancellation: Cancellation,
}
impl ControlHandle {
    async fn send(&self, body: ControlBody) -> Result<ControlReceipt> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(Control { body, reply: tx })
            .await
            .map_err(|_| Error::Cancelled)?;
        rx.await.map_err(|_| Error::Cancelled)?
    }
    pub async fn pause(&self, suspension: Suspension) -> Result<ControlReceipt> {
        suspension.validate()?;
        self.send(ControlBody::Pause(suspension)).await
    }
    pub async fn input(
        &self,
        message: Message,
        source: impl Into<String>,
    ) -> Result<ControlReceipt> {
        if !matches!(message, Message::User { .. } | Message::External { .. }) {
            return Err(Error::Invalid(
                "live input requires user or external content".into(),
            ));
        }
        message.validate()?;
        let source = source.into();
        if source.is_empty() {
            return Err(Error::Invalid("empty input source".into()));
        }
        self.send(ControlBody::Input(message, source)).await
    }
    pub async fn cancel_operation(&self, id: impl Into<String>) -> Result<ControlReceipt> {
        self.send(ControlBody::CancelOperation(id.into())).await
    }
    pub async fn reply_operation(
        &self,
        id: impl Into<String>,
        value: serde_json::Value,
    ) -> Result<ControlReceipt> {
        self.send(ControlBody::ReplyOperation(id.into(), value))
            .await
    }
    pub async fn update_profile(&self, profile: RequestProfile) -> Result<ControlReceipt> {
        self.send(ControlBody::UpdateProfile(profile)).await
    }
    pub async fn interrupt_output(&self) -> Result<ControlReceipt> {
        self.send(ControlBody::InterruptOutput).await
    }
    pub async fn end_input(&self) -> Result<ControlReceipt> {
        self.send(ControlBody::EndInput).await
    }
    /// Cancellation bypasses bounded input queues.
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
}
