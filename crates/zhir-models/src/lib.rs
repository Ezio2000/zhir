//! Protocol-specific model adapters and model composition.
//!
//! User policies belong in [`ProtocolExtension`], created per invocation with
//! `HttpModel::with_extension` for HTTP protocols. MiniMax TTS uses its typed config.
//! Protocol capabilities are defaults, not model
//! discovery; use `HttpModel::with_capabilities` for the selected endpoint.
//!
//! Responses retain their envelope in `provider_data = {protocol, response}`.
//! Only the assistant message/output blocks are replayed on a subsequent turn.
//! For streams, `response` is the assembled value. Each original SSE frame is
//! also emitted as a `ModelDelta::ProtocolEvent` with `{event, id, retry, data}`; `data`
//! is parsed JSON when possible, otherwise a string. This includes known events
//! in addition to their normalized text/tool/usage deltas. The sink owns event
//! retention; the adapter does not accumulate a raw stream transcript.
#[cfg(feature = "anthropic")]
pub mod anthropic;
pub mod capabilities;
mod codec;
pub mod credentials;
pub mod decorators;
#[cfg(any(feature = "minimax", feature = "openai-live"))]
mod native;
pub mod resources;
mod session;
pub use resources::ResourceModel;
pub mod concurrency;
pub use concurrency::ConcurrencyLimitedModel;
mod extension;
pub mod function;
pub mod provider_tools;
pub mod transform;
pub use extension::{ExtensionChain, ExtensionContext, ProtocolExtension};
pub use function::{FunctionDeltaSink, FunctionModel};
pub use transform::TransformModel;
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "openai-live"
))]
pub mod openai;
mod streaming;
pub mod transport;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Chat,
    Responses,
    Messages,
}
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
mod http;
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub mod profiles;
#[cfg(any(
    feature = "openai-chat",
    feature = "openai-responses",
    feature = "anthropic"
))]
pub use http::{HttpModel, ModelConfig};

#[cfg(feature = "minimax")]
pub mod minimax;
#[cfg(feature = "minimax")]
mod websocket;
#[cfg(feature = "minimax")]
pub use websocket::{WebSocketConfig, WebSocketModel};
