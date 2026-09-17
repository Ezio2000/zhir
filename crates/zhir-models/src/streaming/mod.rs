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
pub(crate) use http::*;
