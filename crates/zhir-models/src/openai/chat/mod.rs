use crate::{HttpModel, ModelConfig, Protocol};
pub fn model(config: ModelConfig) -> zhir_core::Result<HttpModel> {
    HttpModel::new(config, Protocol::Chat)
}
