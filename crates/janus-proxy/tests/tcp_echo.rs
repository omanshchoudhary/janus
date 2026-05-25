use janus_proxy::{run_tcp_listener, ListenerConfig};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
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
async fn echoes_bytes_back_to_tcp_client() {
    let addr = next_available_addr();
    let server = tokio::spawn(async move {
        run_tcp_listener(ListenerConfig { listen_addr: addr })
            .await
            .expect("listener should run");
    });

    let mut client = timeout(Duration::from_secs(2), connect_with_retry(addr))
        .await
        .expect("connect timeout");

    client
        .write_all(b"janus")
        .await
        .expect("write to echo server");

    let mut response = [0_u8; 5];
    timeout(Duration::from_secs(2), client.read_exact(&mut response))
        .await
        .expect("read timeout")
        .expect("read echoed bytes");

    assert_eq!(&response, b"janus");

    server.abort();
    let _ = server.await;
}
