//! Test-only signaling forwarder and UDP relay. All nominated ICE traffic crosses
//! the relay, so a blackout drops STUN, DTLS, SCTP and RTP, not just HTTP requests.
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, UdpSocket},
};

#[derive(Default)]
pub struct Traffic {
    pub dropping: AtomicBool,
    pub restored: AtomicBool,
    pub dropped: AtomicUsize,
    pub forwarded_after: AtomicUsize,
    pub creations: AtomicUsize,
    pub client_datagrams: AtomicUsize,
    pub server_datagrams: AtomicUsize,
}
pub struct Relay {
    pub endpoint: String,
    pub traffic: Arc<Traffic>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Relay {
    pub async fn start(client: reqwest::Client, upstream: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/calls", listener.local_addr().unwrap());
        let traffic = Arc::new(Traffic::default());
        let counts = traffic.clone();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            counts.creations.fetch_add(1, Ordering::SeqCst);
            let mut data = vec![];
            let (header_end, length) = loop {
                let mut buf = [0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
                if let Some(end) = data.windows(4).position(|s| s == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&data[..end]).to_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while data.len() < header_end + length {
                let mut buf = [0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                data.extend_from_slice(&buf[..n]);
            }
            let offer: serde_json::Value =
                serde_json::from_slice(&data[header_end..header_end + length]).unwrap();
            let local_ips: std::collections::BTreeSet<std::net::IpAddr> = offer["sdp"]
                .as_str()
                .unwrap()
                .lines()
                .filter_map(|line| line.strip_prefix("a=candidate:"))
                .filter_map(|line| {
                    let fields: Vec<_> = line.split_whitespace().collect();
                    fields
                        .get(4)?
                        .parse::<std::net::IpAddr>()
                        .ok()
                        .filter(std::net::IpAddr::is_ipv4)
                })
                .collect();
            assert!(!local_ips.is_empty());
            let mut request = client.post(upstream);
            for line in std::str::from_utf8(&data[..header_end]).unwrap().lines() {
                if let Some((name, value)) = line.split_once(':')
                    && matches!(
                        name.to_lowercase().as_str(),
                        "authorization"
                            | "chatgpt-account-id"
                            | "openai-alpha"
                            | "originator"
                            | "x-session-id"
                    )
                {
                    request = request.header(name, value.trim());
                }
            }
            let response = request
                .header("content-type", "application/json")
                .body(data[header_end..header_end + length].to_vec())
                .send()
                .await
                .unwrap();
            let status = response.status();
            let sdp = response.text().await.unwrap();
            assert!(status.is_success(), "signaling rejected: {status}");
            let mut routes =
                std::collections::HashMap::<(SocketAddr, std::net::IpAddr), Arc<UdpSocket>>::new();
            let mut answer = String::new();
            for line in sdp.lines() {
                if let Some(candidate) = line.strip_prefix("a=candidate:") {
                    let mut fields: Vec<String> =
                        candidate.split_whitespace().map(str::to_owned).collect();
                    if fields.len() < 8
                        || fields[1] != "1"
                        || !fields[2].eq_ignore_ascii_case("udp")
                    {
                        continue;
                    }
                    let Ok(remote) = format!("{}:{}", fields[4], fields[5]).parse::<SocketAddr>()
                    else {
                        continue;
                    };
                    if !remote.is_ipv4() {
                        continue;
                    }
                    for ip in &local_ips {
                        let frontend = if let Some(socket) = routes.get(&(remote, *ip)) {
                            socket.clone()
                        } else {
                            let socket =
                                Arc::new(UdpSocket::bind(SocketAddr::new(*ip, 0)).await.unwrap());
                            routes.insert((remote, *ip), socket.clone());
                            socket
                        };
                        fields[0] = format!("relay{}", frontend.local_addr().unwrap().port());
                        fields[4] = ip.to_string();
                        fields[5] = frontend.local_addr().unwrap().port().to_string();
                        answer.push_str("a=candidate:");
                        answer.push_str(&fields.join(" "));
                        answer.push_str("\r\n");
                    }
                } else {
                    answer.push_str(line);
                    answer.push_str("\r\n");
                }
            }
            assert!(
                !routes.is_empty(),
                "server SDP must contain IPv4 UDP candidates"
            );
            eprintln!("relay remote candidate count: {}", routes.len());
            socket.write_all(format!("HTTP/1.1 201 Created\r\nContent-Type: application/sdp\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{answer}", answer.len()).as_bytes()).await.unwrap();
            drop(socket);
            let mut routes_running = tokio::task::JoinSet::<()>::new();
            for ((remote, _), udp) in routes {
                let counts = counts.clone();
                routes_running.spawn(async move {
                    let mut clients =
                        std::collections::HashMap::<SocketAddr, tokio::sync::mpsc::Sender<Vec<u8>>>::new();
                    let mut workers = tokio::task::JoinSet::<()>::new();
                    let mut buffer = vec![0; 65536];
                    loop {
                        let (size, from) = udp.recv_from(&mut buffer).await.unwrap();
                        counts.client_datagrams.fetch_add(1, Ordering::SeqCst);
                        if counts.dropping.load(Ordering::SeqCst) {
                            counts.dropped.fetch_add(1, Ordering::SeqCst);
                            continue;
                        }
                        if let std::collections::hash_map::Entry::Vacant(entry) = clients.entry(from) {
                            let backend = UdpSocket::bind(SocketAddr::new(from.ip(), 0)).await.unwrap();
                            backend.connect(remote).await.unwrap();
                            let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
                            entry.insert(tx);
                            let frontend = udp.clone();
                            let counts = counts.clone();
                            workers.spawn(async move {
                                let mut buffer = vec![0; 65536];
                                loop {
                                    tokio::select! {
                                        Some(packet) = rx.recv() => { let _ = backend.send(&packet).await; }
                                        packet = backend.recv(&mut buffer) => {
                                            let Ok(size) = packet else { continue; };
                                            counts.server_datagrams.fetch_add(1, Ordering::SeqCst);
                                            if counts.dropping.load(Ordering::SeqCst) {
                                                counts.dropped.fetch_add(1, Ordering::SeqCst);
                                            } else {
                                                let _ = frontend.send_to(&buffer[..size], from).await;
                                                if counts.restored.load(Ordering::SeqCst) { counts.forwarded_after.fetch_add(1, Ordering::SeqCst); }
                                            }
                                        }
                                    }
                                }
                            });
                        }
                        let _ = clients[&from].send(buffer[..size].to_vec()).await;
                        if counts.restored.load(Ordering::SeqCst) {
                            counts.forwarded_after.fetch_add(1, Ordering::SeqCst);
                        }
                        if let Some(result) = workers.try_join_next() {
                            result.unwrap();
                            panic!("UDP relay worker ended");
                        }
                    }
                });
            }
            while let Some(result) = routes_running.join_next().await {
                result.unwrap();
            }
        });
        Self {
            endpoint,
            traffic,
            task,
        }
    }
}
