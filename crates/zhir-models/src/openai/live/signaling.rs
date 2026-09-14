use super::LiveConfig;
use serde_json::Value;
use zhir_core::{
    Result,
    credential::CredentialContext,
    error::{Error, Failure},
};

pub(super) async fn create(
    config: &LiveConfig,
    session_id: &str,
    sdp: String,
    session: Value,
) -> Result<String> {
    for attempt in 0..2 {
        let credential = config
            .credentials
            .resolve(CredentialContext {
                audience: config.endpoint.clone(),
                now_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| Error::Invalid("clock before Unix epoch".into()))?
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
            })
            .await?;
        let mut headers = reqwest::header::HeaderMap::new();
        let mut auth = reqwest::header::HeaderValue::from_str(&format!(
            "{} {}",
            credential.scheme, credential.value
        ))
        .map_err(|_| Error::Invalid("invalid Live credential".into()))?;
        auth.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, auth);
        headers.insert("OpenAI-Alpha", "quicksilver=v2".parse().expect("constant"));
        headers.insert("originator", "codex_cli_rs".parse().expect("constant"));
        headers.insert(
            "x-session-id",
            session_id
                .parse()
                .map_err(|_| Error::Invalid("invalid session id".into()))?,
        );
        for (key, value) in &credential.metadata {
            if let Some(key) = key.strip_prefix("header:") {
                let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                    .map_err(|_| Error::Invalid("invalid credential metadata header".into()))?;
                if headers.contains_key(&name)
                    || matches!(name.as_str(), "host" | "content-length" | "content-type")
                {
                    return Err(Error::Invalid(
                        "credential metadata overrides a controlled header".into(),
                    ));
                }
                let mut value = reqwest::header::HeaderValue::from_str(value)
                    .map_err(|_| Error::Invalid("invalid credential metadata value".into()))?;
                value.set_sensitive(true);
                headers.insert(name, value);
            }
        }
        if !headers.contains_key("chatgpt-account-id") {
            return Err(Error::Invalid(
                "Live requires header:ChatGPT-Account-Id credential metadata".into(),
            ));
        }
        let response = config
            .http_client
            .post(&config.endpoint)
            .headers(headers)
            .json(&serde_json::json!({"sdp":sdp,"session":session}))
            .send()
            .await
            .map_err(|_| Error::Uncertain("Live creation outcome unknown".into()))?;
        let status = response.status();
        if status.as_u16() == 401 && attempt == 0 {
            config
                .credentials
                .invalidate(&credential.generation)
                .await?;
            continue;
        }
        if !status.is_success() {
            return Err(Error::Model(Failure::new(
                format!("http_{}", status.as_u16()),
                "Live session creation rejected",
            )));
        }
        let mut response = response;
        let mut data = Vec::new();
        while let Some(bytes) = response
            .chunk()
            .await
            .map_err(|_| Error::Uncertain("Live SDP response incomplete".into()))?
        {
            if data.len() + bytes.len() > config.max_event_bytes {
                return Err(Error::Protocol("oversized Live SDP".into()));
            }
            data.extend_from_slice(&bytes);
        }
        return String::from_utf8(data)
            .map_err(|_| Error::Protocol("invalid Live SDP text".into()));
    }
    unreachable!()
}
