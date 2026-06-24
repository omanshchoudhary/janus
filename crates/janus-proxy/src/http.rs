use crate::config::*;
use crate::tcp::{close_connection, connect_backend};
use janus_core::{Backend, BackendRuntime, HttpHeader, HttpRequestHead, HttpResponseHead};
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::Duration,
};

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

// Http connection handler
pub(crate) async fn handle_http_connection(
    client_socket: TcpStream,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    active_connections: Arc<AtomicUsize>,
    backend_runtime: Arc<BackendRuntime>,
    metrics: Arc<ProxyMetrics>,
) {
    let _connection_guard = backend_runtime.begin_connection();
    let mut client_reader = BufReader::new(client_socket);
    let mut head = match read_request_head(&mut client_reader).await {
        Ok(head) => head,
        Err(error) => {
            tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "failed to read request head");
            close_connection(
                client_reader.get_mut(),
                connection_id.0,
                peer_addr,
                active_connections.as_ref(),
            )
            .await;
            return;
        }
    };

    if let Err(error) = reject_unsupported(&head) {
        tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "unsupported request");
        close_connection(
            client_reader.get_mut(),
            connection_id.0,
            peer_addr,
            active_connections.as_ref(),
        )
        .await;
        return;
    }
    strip_hop_by_hop(&mut head);
    add_forwarding_headers(&mut head, peer_addr);
    let backend = backend_runtime.backend();
    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        backend_id = %backend.id.0,
        backend_addr = %backend.address.0,
        "connecting to backend"
    );

    let mut backend_socket = match connect_backend(backend, Duration::from_secs(2)).await {
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
            close_connection(
                client_reader.get_mut(),
                connection_id.0,
                peer_addr,
                active_connections.as_ref(),
            )
            .await;
            return;
        }
    };

    if let Err(error) = backend_socket
        .write_all(serialize_request_head(&head).as_bytes())
        .await
    {
        tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "failed to write request to backend");
        backend_runtime.record_failure();
        close_connection(
            client_reader.get_mut(),
            connection_id.0,
            peer_addr,
            active_connections.as_ref(),
        )
        .await;
        return;
    }

    match content_length(&head.headers) {
        Ok(Some(n)) if n > 0 => {
            let mut body = (&mut client_reader).take(n);
            if let Err(error) = tokio::io::copy(&mut body, &mut backend_socket).await {
                tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "failed to copy request body");
                backend_runtime.record_failure();
                close_connection(
                    client_reader.get_mut(),
                    connection_id.0,
                    peer_addr,
                    active_connections.as_ref(),
                )
                .await;
                return;
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "invalid content-length");
            close_connection(
                client_reader.get_mut(),
                connection_id.0,
                peer_addr,
                active_connections.as_ref(),
            )
            .await;
            return;
        }
    }

    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        backend_id = %backend.id.0,
        "forwarded request to backend"
    );

    let mut backend_reader = BufReader::new(backend_socket);

    let response_head = match read_response_head(&mut backend_reader).await {
        Ok(h) => h,
        Err(error) => {
            tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "failed to read response head");
            backend_runtime.record_failure();
            close_connection(
                client_reader.get_mut(),
                connection_id.0,
                peer_addr,
                active_connections.as_ref(),
            )
            .await;
            return;
        }
    };

    metrics.record_status(response_head.status);
    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        backend_id = %backend.id.0,
        status = response_head.status,
        "received response from backend"
    );

    if let Err(error) = client_reader
        .get_mut()
        .write_all(serialize_response_head(&response_head).as_bytes())
        .await
    {
        tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "failed to write response head to client");
        close_connection(
            client_reader.get_mut(),
            connection_id.0,
            peer_addr,
            active_connections.as_ref(),
        )
        .await;
        return;
    }

    let copy_result = match content_length(&response_head.headers) {
        Ok(Some(n)) if n > 0 => {
            let mut body = (&mut backend_reader).take(n);
            tokio::io::copy(&mut body, client_reader.get_mut()).await
        }
        Ok(_) => tokio::io::copy(&mut backend_reader, client_reader.get_mut()).await,
        Err(error) => {
            tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "invalid response content-length");
            close_connection(
                client_reader.get_mut(),
                connection_id.0,
                peer_addr,
                active_connections.as_ref(),
            )
            .await;
            return;
        }
    };

    if let Err(error) = copy_result {
        tracing::error!(connection_id = connection_id.0, %peer_addr, %error, "failed to copy response body");
    }

    close_connection(
        client_reader.get_mut(),
        connection_id.0,
        peer_addr,
        active_connections.as_ref(),
    )
    .await;
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

fn parse_response_line(line: &str) -> janus_core::Result<HttpResponseHead> {
    let line = line.trim_end_matches(['\r', '\n']);

    let mut parts = line.splitn(3, ' ');
    let version = parts.next();
    let code = parts.next();
    let reason = parts.next().unwrap_or(""); // empty reason is valid

    let (version, code) = match (version, code) {
        (Some(v), Some(c)) => (v, c),
        _ => return Err(janus_core::Error::Protocol("invalid status line".into())),
    };

    let status = code
        .parse::<u16>()
        .map_err(|_| janus_core::Error::Protocol("invalid status code".into()))?;

    Ok(HttpResponseHead {
        version: version.to_string(),
        status,
        reason: reason.to_string(),
        headers: Vec::new(),
    })
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

async fn read_response_head<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> janus_core::Result<HttpResponseHead> {
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let trimmed = line.trim_end_matches(['\r', '\n']);
    let mut head = parse_response_line(trimmed)?;

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

// Handles both req and res
fn content_length(headers: &[HttpHeader]) -> janus_core::Result<Option<u64>> {
    let h = headers
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

fn serialize_response_head(head: &HttpResponseHead) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{} {} {}\r\n",
        head.version, head.status, head.reason
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

#[cfg(test)]
mod tests {
    use super::{
        add_forwarding_headers, content_length, parse_request_line, parse_response_line,
        read_request_head, read_response_head, reject_unsupported, serialize_request_head,
        strip_hop_by_hop,
    };
    use crate::config::{decrement_active_connections, increment_active_connections};
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
        assert_eq!(content_length(&head.headers).unwrap(), None);
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
        assert_eq!(content_length(&head.headers).unwrap(), Some(42));
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
        assert!(content_length(&head.headers).is_err());
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

    #[test]
    fn parses_a_valid_response_line() {
        let head = parse_response_line("HTTP/1.1 200 OK").unwrap();
        assert_eq!(head.version, "HTTP/1.1");
        assert_eq!(head.status, 200);
        assert_eq!(head.reason, "OK");
    }

    #[test]
    fn parses_a_response_line_with_empty_reason() {
        let head = parse_response_line("HTTP/1.1 204 ").unwrap();
        assert_eq!(head.status, 204);
        assert_eq!(head.reason, "");
    }

    #[test]
    fn rejects_a_response_line_with_invalid_status_code() {
        assert!(parse_response_line("HTTP/1.1 abc OK").is_err());
    }

    #[tokio::test]
    async fn reads_a_response_head_with_status_and_headers() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\nServer: janus\r\n\r\n";
        let mut reader = tokio::io::BufReader::new(&raw[..]);
        let head = read_response_head(&mut reader).await.unwrap();
        assert_eq!(head.status, 200);
        assert_eq!(head.headers.len(), 2);
    }
}

fn is_request_retryable(head: &HttpRequestHead, retry: &RetryConfig) -> bool {
    let method_ok = retry
        .retryable_methods
        .iter()
        .any(|m| m.eq_ignore_ascii_case(&head.method));
    let body_replayable = matches!(content_length(&head.headers), Ok(None) | Ok(Some(0)));
    method_ok && body_replayable
}
