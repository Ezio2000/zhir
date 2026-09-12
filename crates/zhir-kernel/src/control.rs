use tokio::sync::mpsc;
use zhir_core::{Result, error::Error, message::Message, run::Suspension};
#[derive(Debug)]
pub(crate) enum Control {
    Pause(Suspension),
    Insert(Message, String),
    CancelTool(String),
    Abort,
}
#[derive(Clone, Debug)]
pub struct ControlHandle {
    pub(crate) sender: mpsc::UnboundedSender<Control>,
}
impl ControlHandle {
    pub fn pause(&self, suspension: Suspension) -> Result<()> {
        suspension.validate()?;
        let _ = self.sender.send(Control::Pause(suspension));
        Ok(())
    }
    pub fn insert(&self, message: Message, source: impl Into<String>) -> Result<()> {
        if !matches!(message, Message::External { .. }) {
            return Err(Error::Invalid(
                "live insertion requires an external message".into(),
            ));
        }
        message.validate()?;
        let source = source.into();
        if source.is_empty() {
            return Err(Error::Invalid("empty insertion source".into()));
        }
        let _ = self.sender.send(Control::Insert(message, source));
        Ok(())
    }
    pub fn cancel_tool(&self, call_id: impl Into<String>) -> Result<()> {
        let id = call_id.into();
        if id.is_empty() {
            return Err(Error::Invalid("empty call id".into()));
        }
        let _ = self.sender.send(Control::CancelTool(id));
        Ok(())
    }
    pub fn cancel(&self) {
        let _ = self.sender.send(Control::Abort);
    }
}
