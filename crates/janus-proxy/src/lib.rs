use janus_balancer::{BackendCandidate, LoadBalancer, RoundRobinBalancer, SelectionContext};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};

use janus_core::{Backend, BackendRuntime, HealthStatus, HttpHeader, HttpRequestHead, Protocol};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// Bytes-in and bytes-out metrics placeholders.
pub struct ProxyMetrics {
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
}

// Janus server's listening address.
pub struct ListenerConfig {
    pub listen_addr: SocketAddr,
}

pub struct ConnectionId(pub u64);

fn increment_active_connections(active_connections: &AtomicUsize) -> usize {
    active_connections.fetch_add(1, Ordering::Relaxed) + 1
}

fn decrement_active_connections(active_connections: &AtomicUsize) -> usize {
    active_connections.fetch_sub(1, Ordering::Relaxed) - 1
}

// Bind our tcp listener to a port.
pub async fn run_tcp_listener(config: ListenerConfig, backend: Backend) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("Listener started");
    serve_tcp_listener(listener, backend).await
}

pub async fn serve_tcp_listener(listener: TcpListener, backend: Backend) -> janus_core::Result<()> {
    let balancer = Arc::new(RoundRobinBalancer::new());
    serve_tcp_listener_multi(listener, vec![backend], balancer, Protocol::Tcp).await
}

pub async fn run_tcp_listener_multi(
    config: ListenerConfig,
    backends: Vec<Backend>,
    balancer: Arc<dyn LoadBalancer + Send + Sync>,
    protocol: Protocol,
) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("Listener started");
    serve_tcp_listener_multi(listener, backends, balancer, protocol).await
}

pub async fn serve_tcp_listener_multi(
    listener: TcpListener,
    backends: Vec<Backend>,
    balancer: Arc<dyn LoadBalancer + Send + Sync>,
    protocol: Protocol,
) -> janus_core::Result<()> {
    let metrics = Arc::new(ProxyMetrics {
        bytes_in: AtomicU64::new(0),
        bytes_out: AtomicU64::new(0),
    });

    let mut next_connection_id = 0u64;
    let active_connections = Arc::new(AtomicUsize::new(0));
    let candidates: Vec<BackendCandidate> = backends
        .into_iter()
        .map(|b| {
            let runtime = Arc::new(BackendRuntime::new(b));
            runtime.set_health(HealthStatus::Healthy);
            BackendCandidate { runtime }
        })
        .collect();

    loop {
        match listener.accept().await {
            // addr is the client address
            Ok((mut socket, addr)) => {
                let ctx = SelectionContext {
                    client_addr: Some(addr),
                };
                let selected = match balancer.select(&candidates, &ctx) {
                    Some(selected) => selected,
                    None => {
                        tracing::warn!(%addr, "no backend available for connection");
                        let _ = socket.shutdown().await;
                        continue;
                    }
                };

                next_connection_id += 1;
                let connection_id = ConnectionId(next_connection_id);
                let current = increment_active_connections(active_connections.as_ref());

                tracing::info!(
                    connection_id = connection_id.0,
                    %addr,
                    active_connections = current,
                    "accepted connection"
                );

                let active_connections_for_task = Arc::clone(&active_connections);
                let metrics_for_task = metrics.clone();
                let backend_runtime_for_task = Arc::clone(&selected.runtime);

                tokio::spawn(async move {
                    match protocol {
                        Protocol::Tcp => {
                            handle_tcp_connection(
                                socket,
                                connection_id,
                                addr,
                                active_connections_for_task,
                                backend_runtime_for_task,
                                metrics_for_task,
                            )
                            .await;
                        }
                        Protocol::Http1 => {
                            tracing::warn!(
                                connection_id = connection_id.0,
                                %addr,
                                "HTTP/1.1 proxying is not implemented yet"
                            );
                            let _ = socket.shutdown().await;
                            let current =
                                decrement_active_connections(active_connections_for_task.as_ref());
                            tracing::info!(
                                connection_id = connection_id.0,
                                %addr,
                                active_connections = current,
                                "closed connection"
                            );
                        }
                    }
                });
            }
            Err(error) => {
                tracing::error!(%error, "failed to accept incoming connection");
            }
        }
    }
}

// Bind our https listener to a port.
pub async fn run_http_listener(config: ListenerConfig, backend: Backend) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    serve_http_listener(listener, backend).await?;
    Ok(())
}

pub async fn serve_http_listener(
    _listener: TcpListener,
    _backend: Backend,
) -> janus_core::Result<()> {
    Ok(())
}

async fn handle_tcp_connection(
    mut client_socket: TcpStream,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    active_connections: Arc<AtomicUsize>,
    backend_runtime: Arc<BackendRuntime>,
    metrics: Arc<ProxyMetrics>,
) {
    let _connection_guard = backend_runtime.begin_connection();
    let backend = backend_runtime.backend();

    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        backend_id = %backend.id.0,
        backend_addr = %backend.address.0,
        "connecting to backend"
    );

    let started_at = std::time::Instant::now();
    let mut backend_socket = match connect_backend(&backend, Duration::from_secs(2)).await {
        Ok(stream) => stream,
        Err(error) => {
            tracing::error!(
                connection_id = connection_id.0,
                %peer_addr,
                backend_id = %backend.id.0,
                %error,
                "failed to connect to backend"
            );
            backend_runtime.record_failure();

            let _ = client_socket.shutdown().await;

            let current = decrement_active_connections(active_connections.as_ref());
            tracing::info!(
                connection_id = connection_id.0,
                %peer_addr,
                active_connections = current,
                "closed connection"
            );
            return;
        }
    };

    match tokio::io::copy_bidirectional(&mut client_socket, &mut backend_socket).await {
        Ok((client_to_backend_bytes, backend_to_client_bytes)) => {
            metrics
                .bytes_in
                .fetch_add(client_to_backend_bytes, Ordering::Relaxed);

            metrics
                .bytes_out
                .fetch_add(backend_to_client_bytes, Ordering::Relaxed);

            let total_bytes_in = metrics.bytes_in.load(Ordering::Relaxed);
            let total_bytes_out = metrics.bytes_out.load(Ordering::Relaxed);
            let duration = started_at.elapsed();
            tracing::info!(
                connection_id = connection_id.0,
                %peer_addr,
                backend_id = %backend.id.0,
                client_to_backend_bytes,
                backend_to_client_bytes,
                total_bytes_in,
                total_bytes_out,
                ?duration,
                "forwarded tcp connection"
            );
        }
        Err(error) => {
            tracing::error!(
                connection_id = connection_id.0,
                %peer_addr,
                backend_id = %backend.id.0,
                %error,
                "tcp forwarding failed"
            );
            backend_runtime.record_failure();
        }
    }

    let _ = client_socket.shutdown().await;

    let current = decrement_active_connections(active_connections.as_ref());
    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        active_connections = current,
        "closed connection"
    );
}

async fn connect_backend(
    backend: &Backend,
    timeout_duration: Duration,
) -> janus_core::Result<TcpStream> {
    match timeout(timeout_duration, TcpStream::connect(backend.address.0)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(janus_core::Error::Timeout),
    }
}

pub fn janus_proxy() -> &'static str {
    "janus-proxy"
}

fn parse_request_line(line: &str) -> janus_core::Result<HttpRequestHead> {
    let line = line.trim_end_matches("\r\n");

    let parts: Vec<&str> = line.split(' ').collect();

    if parts.len() != 3 {
        return Err(janus_core::Error::Protocol("invalid request line".into()));
    }
    let method = parts[0];
    let target = parts[1];
    let version = parts[2];

    validate_target(target)?;

    Ok(HttpRequestHead {
        method: method.to_string(),
        target: target.to_string(),
        version: version.to_string(),
        headers: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        add_forwarding_headers, content_length, decrement_active_connections,
        increment_active_connections, parse_request_line, read_request_head, reject_unsupported,
        serialize_request_head, strip_hop_by_hop,
    };
    use janus_core::{HttpHeader, HttpRequestHead};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn active_connection_counter_increments_and_decrements() {
        let active_connections = AtomicUsize::new(0);

        let first = increment_active_connections(&active_connections);
        let second = increment_active_connections(&active_connections);
        let remaining = decrement_active_connections(&active_connections);

        assert_eq!(first, 1);
        assert_eq!(second, 2);
        assert_eq!(remaining, 1);
        assert_eq!(active_connections.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parses_a_valid_request_line() {
        let head = parse_request_line("GET /index.html HTTP/1.1\r\n").unwrap();
        assert_eq!(head.method, "GET");
        assert_eq!(head.target, "/index.html");
        assert_eq!(head.version, "HTTP/1.1");
    }

    #[test]
    fn rejects_too_few_tokens() {
        assert!(parse_request_line("GET /\r\n").is_err());
    }

    #[test]
    fn rejects_target_without_leading_slash() {
        assert!(parse_request_line("GET index.html HTTP/1.1\r\n").is_err());
    }

    #[test]
    fn rejects_absolute_form_target() {
        assert!(parse_request_line("GET http://x/ HTTP/1.1\r\n").is_err());
    }

    #[tokio::test]
    async fn reads_request_line_and_headers() {
        use tokio::io::BufReader;
        let raw = b"GET / HTTP/1.1\r\nHost: localhost:8080\r\n\r\n";
        let mut r = BufReader::new(&raw[..]);
        let head = read_request_head(&mut r).await.unwrap();
        assert_eq!(head.method, "GET");
        assert_eq!(head.headers.len(), 1);
        assert_eq!(head.headers[0].name, "Host");
        assert_eq!(head.headers[0].value, "localhost:8080");
    }

    #[test]
    fn content_length_absent_is_none() {
        let head = HttpRequestHead {
            method: "GET".into(),
            target: "/".into(),
            version: "HTTP/1.1".into(),
            headers: vec![],
        };
        assert_eq!(content_length(&head).unwrap(), None);
    }

    #[test]
    fn content_length_parsed_case_insensitive() {
        let head = HttpRequestHead {
            method: "POST".into(),
            target: "/".into(),
            version: "HTTP/1.1".into(),
            headers: vec![HttpHeader {
                name: "content-length".into(),
                value: "42".into(),
            }],
        };
        assert_eq!(content_length(&head).unwrap(), Some(42));
    }

    #[test]
    fn content_length_invalid_is_err() {
        let head = HttpRequestHead {
            method: "POST".into(),
            target: "/".into(),
            version: "HTTP/1.1".into(),
            headers: vec![HttpHeader {
                name: "Content-Length".into(),
                value: "abc".into(),
            }],
        };
        assert!(content_length(&head).is_err());
    }

    fn head_with(headers: Vec<HttpHeader>) -> HttpRequestHead {
        HttpRequestHead {
            method: "GET".into(),
            target: "/".into(),
            version: "HTTP/1.1".into(),
            headers,
        }
    }

    #[test]
    fn accepts_plain_request() {
        let head = head_with(vec![HttpHeader {
            name: "Host".into(),
            value: "x".into(),
        }]);
        assert!(reject_unsupported(&head).is_ok());
    }

    #[test]
    fn rejects_transfer_encoding() {
        let head = head_with(vec![HttpHeader {
            name: "Transfer-Encoding".into(),
            value: "chunked".into(),
        }]);
        assert!(reject_unsupported(&head).is_err());
    }

    #[test]
    fn rejects_upgrade() {
        let head = head_with(vec![HttpHeader {
            name: "Upgrade".into(),
            value: "websocket".into(),
        }]);
        assert!(reject_unsupported(&head).is_err());
    }

    #[test]
    fn rejects_transfer_encoding_case_insensitive() {
        let head = head_with(vec![HttpHeader {
            name: "transfer-encoding".into(),
            value: "chunked".into(),
        }]);
        assert!(reject_unsupported(&head).is_err());
    }

    fn header_names(head: &HttpRequestHead) -> Vec<String> {
        head.headers.iter().map(|h| h.name.clone()).collect()
    }

    #[test]
    fn strips_hop_by_hop_headers() {
        let mut head = head_with(vec![
            HttpHeader {
                name: "Host".into(),
                value: "x".into(),
            },
            HttpHeader {
                name: "Connection".into(),
                value: "keep-alive".into(),
            },
            HttpHeader {
                name: "Proxy-Connection".into(),
                value: "keep-alive".into(),
            },
            HttpHeader {
                name: "Keep-Alive".into(),
                value: "timeout=5".into(),
            },
            HttpHeader {
                name: "Transfer-Encoding".into(),
                value: "chunked".into(),
            },
        ]);
        strip_hop_by_hop(&mut head);
        assert_eq!(header_names(&head), vec!["Host"]);
    }

    #[test]
    fn preserves_ordinary_headers() {
        let mut head = head_with(vec![
            HttpHeader {
                name: "Host".into(),
                value: "x".into(),
            },
            HttpHeader {
                name: "User-Agent".into(),
                value: "curl".into(),
            },
        ]);
        strip_hop_by_hop(&mut head);
        assert_eq!(header_names(&head), vec!["Host", "User-Agent"]);
    }

    #[test]
    fn strips_hop_by_hop_case_insensitive() {
        let mut head = head_with(vec![
            HttpHeader {
                name: "Host".into(),
                value: "x".into(),
            },
            HttpHeader {
                name: "CONNECTION".into(),
                value: "close".into(),
            },
        ]);
        strip_hop_by_hop(&mut head);
        assert_eq!(header_names(&head), vec!["Host"]);
    }

    fn header_value<'a>(head: &'a HttpRequestHead, name: &str) -> Option<&'a str> {
        head.headers
            .iter()
            .find(|h| h.name.eq_ignore_ascii_case(name))
            .map(|h| h.value.as_str())
    }

    #[test]
    fn adds_forwarding_headers_with_host() {
        let mut head = head_with(vec![HttpHeader {
            name: "Host".into(),
            value: "example.com".into(),
        }]);
        let client = "1.2.3.4:5678".parse().unwrap();
        add_forwarding_headers(&mut head, client);

        assert_eq!(header_value(&head, "X-Forwarded-For"), Some("1.2.3.4"));
        assert_eq!(header_value(&head, "X-Forwarded-Host"), Some("example.com"));
        assert_eq!(header_value(&head, "X-Forwarded-Proto"), Some("http"));
    }

    #[test]
    fn adds_forwarding_headers_without_host() {
        let mut head = head_with(vec![]);
        let client = "1.2.3.4:5678".parse().unwrap();
        add_forwarding_headers(&mut head, client);

        assert_eq!(header_value(&head, "X-Forwarded-For"), Some("1.2.3.4"));
        assert_eq!(header_value(&head, "X-Forwarded-Proto"), Some("http"));
        assert_eq!(header_value(&head, "X-Forwarded-Host"), None);
    }

    #[test]
    fn serializes_request_head() {
        let head = head_with(vec![HttpHeader {
            name: "Host".into(),
            value: "x".into(),
        }]);
        assert_eq!(
            serialize_request_head(&head),
            "GET / HTTP/1.1\r\nHost: x\r\n\r\n"
        );
    }
}

fn parse_header_line(line: &str) -> janus_core::Result<HttpHeader> {
    let raw = line.trim();
    let (name, value) = raw
        .split_once(":")
        .ok_or_else(|| janus_core::Error::Protocol("malformed header".into()))?;
    Ok(HttpHeader {
        name: name.trim().to_string(),
        value: value.trim().to_string(),
    })
}

async fn read_request_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> janus_core::Result<HttpRequestHead> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let mut head = parse_request_line(trimmed)?;

    loop {
        line.clear();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Err(janus_core::Error::Protocol(
                "unexpected eof in headers".into(),
            ));
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break; // Blank line, meaning end of headers
        }
        head.headers.push(parse_header_line(trimmed)?);
    }

    Ok(head)
}

fn validate_target(target: &str) -> janus_core::Result<()> {
    if target.is_empty() || !target.starts_with('/') {
        return Err(janus_core::Error::Protocol("invalid request target".into()));
    }
    Ok(())
}

fn content_length(head: &HttpRequestHead) -> janus_core::Result<Option<u64>> {
    let h = head
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("content-length"));

    match h {
        None => Ok(None),
        Some(h) => h
            .value
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| janus_core::Error::Protocol("invalid content-length".into())),
    }
}

fn reject_unsupported(head: &HttpRequestHead) -> janus_core::Result<()> {
    let has = |name: &str| {
        head.headers
            .iter()
            .any(|h| h.name.eq_ignore_ascii_case(name))
    };

    if has("transfer-encoding") {
        return Err(janus_core::Error::Protocol(
            "transfer-encoding not supported".into(),
        ));
    }
    if has("upgrade") {
        return Err(janus_core::Error::Protocol("upgrade not supported".into()));
    }
    Ok(())
}

fn strip_hop_by_hop(head: &mut HttpRequestHead) {
    const HOP_BY_HOP: [&str; 4] = [
        "connection",
        "proxy-connection",
        "keep-alive",
        "transfer-encoding",
    ];
    head.headers.retain(|h| {
        !HOP_BY_HOP
            .iter()
            .any(|hbh| h.name.eq_ignore_ascii_case(hbh))
    });
}

// Adding forward headers to the connection request before passing it to the backend
fn add_forwarding_headers(head: &mut HttpRequestHead, client_address: SocketAddr) {
    head.headers.push(HttpHeader {
        name: "X-Forwarded-For".into(),
        value: client_address.ip().to_string(),
    });

    if let Some(host) = head
        .headers
        .iter()
        .find(|h| h.name.eq_ignore_ascii_case("host"))
        .map(|h| h.value.clone())
    {
        head.headers.push(HttpHeader {
            name: "X-Forwarded-Host".into(),
            value: host,
        });
    }
    head.headers.push(HttpHeader {
        name: "X-Forwarded-Proto".into(),
        value: "http".into(),
    });
}

// After parsing the incoming client request, and modifying it, it's time to send back to the backend in the original format.
fn serialize_request_head(head: &HttpRequestHead) -> String {
    let mut out = String::new();

    out.push_str(&format!(
        "{} {} {}\r\n",
        head.method, head.target, head.version
    ));
    head.headers.iter().for_each(|header| {
        out.push_str(&header.name);
        out.push_str(": ");
        out.push_str(&header.value);
        out.push_str("\r\n");
    });

    out.push_str("\r\n");
    out
}
