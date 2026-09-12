use std::sync::Mutex;
use zhir_core::{
    BoxFuture, Result,
    error::Error,
    model::{DeltaSink, ModelDelta},
};
#[derive(Default)]
pub struct RecordingSink {
    deltas: Mutex<Vec<ModelDelta>>,
    failure: Option<(usize, Error)>,
}
impl RecordingSink {
    /// Record the selected emission before failing it. Ordinals start at one.
    pub fn failing_on(ordinal: usize, error: Error) -> Result<Self> {
        if ordinal == 0 {
            return Err(Error::Invalid("emission ordinal must be positive".into()));
        }
        Ok(Self {
            deltas: Mutex::new(vec![]),
            failure: Some((ordinal, error)),
        })
    }
    pub fn deltas(&self) -> Vec<ModelDelta> {
        self.deltas.lock().expect("sink records lock").clone()
    }
}
impl DeltaSink for RecordingSink {
    fn emit(&self, delta: ModelDelta) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            let mut deltas = self.deltas.lock().expect("sink records lock");
            deltas.push(delta);
            if let Some((ordinal, error)) = &self.failure
                && *ordinal == deltas.len()
            {
                return Err(error.clone());
            }
            Ok(())
        })
    }
}
