//! Scripted HTTP/1.1 responses. Bodies and protocol assertions belong to the consumer.
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use zhir_core::{Result, error::Error};

#[derive(Debug, Clone)]
pub struct HttpReply {
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub fragment_bytes: usize,
    pub delay: Duration,
    pub fragment_delay: Duration,
    pub disconnect_after: Option<usize>,
}
impl HttpReply {
    pub fn bytes(content_type: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            headers: [("content-type".into(), content_type.into())].into(),
            body: body.into(),
            fragment_bytes: usize::MAX,
            delay: Duration::ZERO,
            fragment_delay: Duration::ZERO,
            disconnect_after: None,
        }
    }
    pub fn json(body: &serde_json::Value) -> Self {
        Self::bytes("application/json", body.to_string().into_bytes())
    }
    /// Accept complete, caller-authored SSE frames without interpreting their semantics.
    pub fn sse(frames: impl Into<String>) -> Self {
        Self::bytes("text/event-stream", frames.into().into_bytes())
    }
    pub fn status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers
            .insert(name.into().to_ascii_lowercase(), value.into());
        self
    }
    pub fn fragment_bytes(mut self, bytes: usize) -> Self {
        self.fragment_bytes = bytes;
        self
    }
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
    pub fn fragment_delay(mut self, delay: Duration) -> Self {
        self.fragment_delay = delay;
        self
    }
    /// Send a full Content-Length header and then close after this many body bytes.
    pub fn disconnect_after(mut self, bytes: usize) -> Self {
        self.disconnect_after = Some(bytes);
        self
    }
}

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub target: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
impl HttpRequest {
    pub fn json(&self) -> Result<serde_json::Value> {
        serde_json::from_slice(&self.body).map_err(|e| invalid(e.to_string()))
    }
}

pub struct HttpFixture {
    url: String,
    task: Option<JoinHandle<Result<Vec<HttpRequest>>>>,
}
impl HttpFixture {
    pub async fn start(replies: impl IntoIterator<Item = HttpReply>) -> Result<Self> {
        Self::with_timeout(replies, Duration::from_secs(10)).await
    }
    /// Bounds each accept/read/write exchange, including deliberate response delays.
    pub async fn with_timeout(
        replies: impl IntoIterator<Item = HttpReply>,
        timeout: Duration,
    ) -> Result<Self> {
        let replies = replies.into_iter().collect::<Vec<_>>();
        if timeout.is_zero()
            || replies.iter().any(|r| {
                r.fragment_bytes == 0
                    || !(100..=599).contains(&r.status)
                    || r.headers.iter().any(|(k, v)| {
                        k.contains(['\r', '\n', ':'])
                            || k.is_empty()
                            || v.contains(['\r', '\n'])
                            || k.eq_ignore_ascii_case("content-length")
                            || k.eq_ignore_ascii_case("transfer-encoding")
                            || k.eq_ignore_ascii_case("connection")
                    })
            })
        {
            return Err(invalid("invalid HTTP fixture configuration"));
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(io)?;
        let url = format!("http://{}", listener.local_addr().map_err(io)?);
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (index, reply) in replies.into_iter().enumerate() {
                let request = tokio::time::timeout(timeout, async {
                    let (mut socket, _) = listener.accept().await.map_err(io)?;
                    let request = read_request(&mut socket).await?;
                    if !reply.delay.is_zero() {
                        tokio::time::sleep(reply.delay).await;
                    }
                    let mut header = format!(
                        "HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n",
                        reply.status,
                        reply.body.len()
                    );
                    for (key, value) in reply.headers {
                        header.push_str(&format!("{key}: {value}\r\n"));
                    }
                    header.push_str("\r\n");
                    socket.write_all(header.as_bytes()).await.map_err(io)?;
                    let end = reply
                        .disconnect_after
                        .unwrap_or(reply.body.len())
                        .min(reply.body.len());
                    for chunk in reply.body[..end].chunks(reply.fragment_bytes) {
                        socket.write_all(chunk).await.map_err(io)?;
                        if !reply.fragment_delay.is_zero() {
                            tokio::time::sleep(reply.fragment_delay).await;
                        }
                    }
                    socket.shutdown().await.map_err(io)?;
                    Ok::<_, Error>(request)
                })
                .await
                .map_err(|_| invalid(format!("HTTP fixture exchange {index} timed out")))??;
                requests.push(request);
            }
            Ok(requests)
        });
        Ok(Self {
            url,
            task: Some(task),
        })
    }
    pub fn url(&self) -> &str {
        &self.url
    }
    /// Wait for every scripted response to be consumed. Errors and unused replies fail here.
    pub async fn finish(mut self) -> Result<Vec<HttpRequest>> {
        let result = self
            .task
            .as_mut()
            .expect("fixture task is owned")
            .await
            .map_err(|e| invalid(format!("HTTP fixture task: {e}")))?;
        self.task.take();
        result
    }
}
impl Drop for HttpFixture {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
fn io(error: std::io::Error) -> Error {
    invalid(format!("HTTP fixture: {error}"))
}
async fn read_request(socket: &mut TcpStream) -> Result<HttpRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 8192];
    let end = loop {
        let n = socket.read(&mut buffer).await.map_err(io)?;
        if n == 0 {
            return Err(invalid("HTTP request closed before headers"));
        }
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(index) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header = std::str::from_utf8(&bytes[..end]).map_err(|e| invalid(e.to_string()))?;
    let mut lines = header.lines();
    let mut first = lines.next().unwrap_or_default().split_whitespace();
    let method = first
        .next()
        .ok_or_else(|| invalid("missing HTTP method"))?
        .to_owned();
    let target = first
        .next()
        .ok_or_else(|| invalid("missing HTTP target"))?
        .to_owned();
    if first.next() != Some("HTTP/1.1") || first.next().is_some() {
        return Err(invalid("expected HTTP/1.1 request"));
    }
    let mut headers = BTreeMap::new();
    for line in lines.filter(|l| !l.is_empty()) {
        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| invalid("invalid HTTP header"))?;
        let key = key.trim().to_ascii_lowercase();
        if headers
            .insert(key.clone(), value.trim().to_owned())
            .is_some()
        {
            return Err(invalid(format!("duplicate HTTP header {key}")));
        }
    }
    // This fixture intentionally accepts fixed-length requests only, as emitted by
    // the JSON transports. Unsupported framing fails explicitly instead of hanging.
    if headers.contains_key("transfer-encoding") {
        return Err(invalid(
            "HTTP fixture requires Content-Length request framing",
        ));
    }
    let size = headers
        .get("content-length")
        .map(|n| n.parse::<usize>())
        .transpose()
        .map_err(|e| invalid(e.to_string()))?
        .unwrap_or(0);
    while bytes.len() - end < size {
        let n = socket.read(&mut buffer).await.map_err(io)?;
        if n == 0 {
            return Err(invalid("HTTP request closed before body"));
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    Ok(HttpRequest {
        method,
        target,
        headers,
        body: bytes[end..end + size].to_vec(),
    })
}
