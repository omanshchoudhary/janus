use janus_balancer::RoundRobinBalancer;
use janus_core::{Backend, BackendAddress, BackendId, Protocol};
use janus_proxy::{run_tcp_listener_multi, ListenerConfig};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout, Duration},
};

fn next_available_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

async fn connect_with_retry(addr: SocketAddr) -> TcpStream {
    for _ in 0..20 {
        match TcpStream::connect(addr).await {
            Ok(stream) => return stream,
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }

    panic!("listener did not accept connections in time");
}

fn make_backend(id: &str, addr: SocketAddr) -> Backend {
    Backend {
        id: BackendId(id.to_string()),
        address: BackendAddress(addr),
        weight: 1,
    }
}

fn spawn_http_proxy(proxy_addr: SocketAddr, backends: Vec<Backend>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let balancer = Arc::new(RoundRobinBalancer::new());
        run_tcp_listener_multi(
            ListenerConfig {
                listen_addr: proxy_addr,
            },
            backends,
            balancer,
            Protocol::Http1,
        )
        .await
        .expect("proxy listener should run");
    })
}

fn content_length_of(head: &str) -> usize {
    for line in head.lines() {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                return value.trim().parse().unwrap_or(0);
            }
        }
    }
    0
}

/// Accepts a single connection, captures the full request (head + body) the
/// proxy forwards, replies with `response`, then closes. The captured request
/// is sent back over the returned receiver so tests can assert on it.
async fn start_http_backend(
    addr: SocketAddr,
    response: &'static str,
) -> (JoinHandle<()>, oneshot::Receiver<String>) {
    let listener = TcpListener::bind(addr).await.expect("bind mock http backend");
    let (tx, rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accept backend connection");
        let mut reader = BufReader::new(socket);

        let mut raw = String::new();
        loop {
            let mut line = String::new();
            let n = reader.read_line(&mut line).await.expect("read request line");
            if n == 0 {
                break;
            }
            let blank = line == "\r\n";
            raw.push_str(&line);
            if blank {
                break;
            }
        }

        let body_len = content_length_of(&raw);
        if body_len > 0 {
            let mut body = vec![0_u8; body_len];
            reader.read_exact(&mut body).await.expect("read request body");
            raw.push_str(&String::from_utf8_lossy(&body));
        }

        let _ = tx.send(raw);

        reader
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        let _ = reader.get_mut().shutdown().await;
    });

    (handle, rx)
}

async fn send_request_and_read_response(proxy_addr: SocketAddr, request: &[u8]) -> String {
    let mut client = timeout(Duration::from_secs(2), connect_with_retry(proxy_addr))
        .await
        .expect("connect timeout");
    client.write_all(request).await.expect("write request");

    let mut response = Vec::new();
    timeout(Duration::from_secs(2), client.read_to_end(&mut response))
        .await
        .expect("read timeout")
        .expect("read proxied response");
    String::from_utf8_lossy(&response).into_owned()
}

#[tokio::test]
async fn proxies_a_get_request() {
    let backend_addr = next_available_addr();
    let (backend, captured) = start_http_backend(
        backend_addr,
        "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
    )
    .await;

    let proxy_addr = next_available_addr();
    let proxy = spawn_http_proxy(proxy_addr, vec![make_backend("b1", backend_addr)]);

    let response = send_request_and_read_response(
        proxy_addr,
        b"GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"), "status: {response}");
    assert!(response.ends_with("hello"), "body: {response}");

    let request = timeout(Duration::from_secs(2), captured)
        .await
        .expect("captured timeout")
        .expect("backend captured request");
    assert!(request.starts_with("GET /hello HTTP/1.1"), "request: {request}");

    backend.await.expect("backend task panicked");
    proxy.abort();
    let _ = proxy.await;
}

#[tokio::test]
async fn proxies_a_post_request_with_content_length() {
    let backend_addr = next_available_addr();
    let (backend, captured) = start_http_backend(
        backend_addr,
        "HTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nOK",
    )
    .await;

    let proxy_addr = next_available_addr();
    let proxy = spawn_http_proxy(proxy_addr, vec![make_backend("b1", backend_addr)]);

    let response = send_request_and_read_response(
        proxy_addr,
        b"POST /submit HTTP/1.1\r\nHost: example.com\r\nContent-Length: 11\r\n\r\nhello world",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 201 Created"), "status: {response}");
    assert!(response.ends_with("OK"), "body: {response}");

    let request = timeout(Duration::from_secs(2), captured)
        .await
        .expect("captured timeout")
        .expect("backend captured request");
    assert!(request.starts_with("POST /submit HTTP/1.1"), "request: {request}");
    assert!(request.ends_with("hello world"), "forwarded body: {request}");

    backend.await.expect("backend task panicked");
    proxy.abort();
    let _ = proxy.await;
}

#[tokio::test]
async fn closes_client_when_backend_is_unavailable() {
    // Nothing listens on this address, so the proxy's connect will fail.
    let unavailable_backend_addr = next_available_addr();
    let proxy_addr = next_available_addr();
    let proxy = spawn_http_proxy(proxy_addr, vec![make_backend("down", unavailable_backend_addr)]);

    let response = send_request_and_read_response(
        proxy_addr,
        b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
    )
    .await;

    assert!(response.is_empty(), "expected closed connection, got: {response}");

    proxy.abort();
    let _ = proxy.await;
}

#[tokio::test]
async fn rewrites_headers_before_forwarding() {
    let backend_addr = next_available_addr();
    let (backend, captured) = start_http_backend(
        backend_addr,
        "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    )
    .await;

    let proxy_addr = next_available_addr();
    let proxy = spawn_http_proxy(proxy_addr, vec![make_backend("b1", backend_addr)]);

    let _ = send_request_and_read_response(
        proxy_addr,
        b"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: keep-alive\r\n\r\n",
    )
    .await;

    let request = timeout(Duration::from_secs(2), captured)
        .await
        .expect("captured timeout")
        .expect("backend captured request");

    // Forwarding headers are added.
    assert!(
        request.contains("X-Forwarded-For: 127.0.0.1"),
        "missing X-Forwarded-For: {request}"
    );
    assert!(
        request.contains("X-Forwarded-Host: example.com"),
        "missing X-Forwarded-Host: {request}"
    );
    assert!(
        request.contains("X-Forwarded-Proto: http"),
        "missing X-Forwarded-Proto: {request}"
    );
    // Hop-by-hop headers are stripped.
    assert!(
        !request.to_ascii_lowercase().contains("connection:"),
        "hop-by-hop header not stripped: {request}"
    );

    backend.await.expect("backend task panicked");
    proxy.abort();
    let _ = proxy.await;
}
