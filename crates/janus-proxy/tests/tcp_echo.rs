use janus_core::{Backend, BackendAddress, BackendId};
use janus_proxy::{run_tcp_listener, ListenerConfig};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
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

#[tokio::test]
async fn proxies_bytes_to_backend_and_back() {
    let backend_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind backend listener");
    let backend_addr = backend_listener
        .local_addr()
        .expect("read backend listener addr");

    let backend = tokio::spawn(async move {
        let (mut socket, _) = backend_listener
            .accept()
            .await
            .expect("accept backend connection");

        let mut buffer = [0_u8; 5];
        socket
            .read_exact(&mut buffer)
            .await
            .expect("read proxied bytes");
        socket.write_all(&buffer).await.expect("write echoed bytes");
    });

    let proxy_addr = next_available_addr();
    let proxy = tokio::spawn(async move {
        let backend = Backend {
            id: BackendId("backend-1".to_string()),
            address: BackendAddress(backend_addr),
            weight: 1,
        };

        run_tcp_listener(
            ListenerConfig {
                listen_addr: proxy_addr,
            },
            backend,
        )
        .await
        .expect("proxy listener should run");
    });

    let mut client = timeout(Duration::from_secs(2), connect_with_retry(proxy_addr))
        .await
        .expect("connect timeout");

    client.write_all(b"janus").await.expect("write to proxy");
    client.shutdown().await.expect("shutdown client");

    let mut response = [0_u8; 5];
    timeout(Duration::from_secs(2), client.read_exact(&mut response))
        .await
        .expect("read timeout")
        .expect("read proxied response");

    assert_eq!(&response, b"janus");

    backend.await.expect("backend task panicked");
    proxy.abort();
    let _ = proxy.await;
}
