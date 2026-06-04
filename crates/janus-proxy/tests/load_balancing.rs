use janus_balancer::{IpHashBalancer, RoundRobinBalancer, WeightedRoundRobinBalancer};
use janus_core::{Backend, BackendAddress, BackendId, Protocol};
use janus_proxy::{run_tcp_listener_multi, ListenerConfig};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, Duration},
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

async fn start_mock_backend(
    addr: SocketAddr,
    response: &'static str,
) -> tokio::task::JoinHandle<()> {
    let listener = TcpListener::bind(addr).await.expect("bind mock backend");
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut buffer = [0_u8; 1024];
            if let Ok(n) = socket.read(&mut buffer).await {
                if n > 0 {
                    let _ = socket.write_all(response.as_bytes()).await;
                }
            }
        }
    })
}

#[tokio::test]
async fn test_round_robin_integration() {
    let addr1 = next_available_addr();
    let addr2 = next_available_addr();

    let backend_task1 = start_mock_backend(addr1, "backend-1").await;
    let backend_task2 = start_mock_backend(addr2, "backend-2").await;

    let proxy_addr = next_available_addr();
    let proxy = tokio::spawn(async move {
        let b1 = Backend {
            id: BackendId("b1".to_string()),
            address: BackendAddress(addr1),
            weight: 1,
        };
        let b2 = Backend {
            id: BackendId("b2".to_string()),
            address: BackendAddress(addr2),
            weight: 1,
        };

        let balancer = Arc::new(RoundRobinBalancer::new());
        run_tcp_listener_multi(
            ListenerConfig {
                listen_addr: proxy_addr,
            },
            vec![b1, b2],
            balancer,
            Protocol::Tcp,
        )
        .await
        .expect("proxy listener should run");
    });

    let mut client1 = connect_with_retry(proxy_addr).await;
    client1.write_all(b"hello").await.unwrap();
    let mut resp1 = [0_u8; 9];
    client1.read_exact(&mut resp1).await.unwrap();
    assert_eq!(&resp1, b"backend-1");

    let mut client2 = connect_with_retry(proxy_addr).await;
    client2.write_all(b"hello").await.unwrap();
    let mut resp2 = [0_u8; 9];
    client2.read_exact(&mut resp2).await.unwrap();
    assert_eq!(&resp2, b"backend-2");

    backend_task1.abort();
    backend_task2.abort();
    proxy.abort();
}

#[tokio::test]
async fn test_weighted_round_robin_integration() {
    let addr1 = next_available_addr();
    let addr2 = next_available_addr();

    let backend_task1 = start_mock_backend(addr1, "backend-1").await;
    let backend_task2 = start_mock_backend(addr2, "backend-2").await;

    let proxy_addr = next_available_addr();
    let proxy = tokio::spawn(async move {
        let b1 = Backend {
            id: BackendId("b1".to_string()),
            address: BackendAddress(addr1),
            weight: 1,
        };
        let b2 = Backend {
            id: BackendId("b2".to_string()),
            address: BackendAddress(addr2),
            weight: 2,
        };

        let balancer = Arc::new(WeightedRoundRobinBalancer::new());
        run_tcp_listener_multi(
            ListenerConfig {
                listen_addr: proxy_addr,
            },
            vec![b1, b2],
            balancer,
            Protocol::Tcp,
        )
        .await
        .expect("proxy listener should run");
    });

    let mut results = Vec::new();
    for _ in 0..3 {
        let mut client = connect_with_retry(proxy_addr).await;
        client.write_all(b"hello").await.unwrap();
        let mut resp = [0_u8; 9];
        client.read_exact(&mut resp).await.unwrap();
        results.push(String::from_utf8(resp.to_vec()).unwrap());
    }

    assert_eq!(results, vec!["backend-1", "backend-2", "backend-2"]);

    backend_task1.abort();
    backend_task2.abort();
    proxy.abort();
}

#[tokio::test]
async fn test_ip_hash_integration() {
    let addr1 = next_available_addr();
    let addr2 = next_available_addr();

    let backend_task1 = start_mock_backend(addr1, "backend-1").await;
    let backend_task2 = start_mock_backend(addr2, "backend-2").await;

    let proxy_addr = next_available_addr();
    let proxy = tokio::spawn(async move {
        let b1 = Backend {
            id: BackendId("b1".to_string()),
            address: BackendAddress(addr1),
            weight: 1,
        };
        let b2 = Backend {
            id: BackendId("b2".to_string()),
            address: BackendAddress(addr2),
            weight: 1,
        };

        let balancer = Arc::new(IpHashBalancer::new());
        run_tcp_listener_multi(
            ListenerConfig {
                listen_addr: proxy_addr,
            },
            vec![b1, b2],
            balancer,
            Protocol::Tcp,
        )
        .await
        .expect("proxy listener should run");
    });

    let mut client1 = connect_with_retry(proxy_addr).await;
    client1.write_all(b"hello").await.unwrap();
    let mut resp1 = [0_u8; 9];
    client1.read_exact(&mut resp1).await.unwrap();

    let mut client2 = connect_with_retry(proxy_addr).await;
    client2.write_all(b"hello").await.unwrap();
    let mut resp2 = [0_u8; 9];
    client2.read_exact(&mut resp2).await.unwrap();

    assert_eq!(resp1, resp2);

    backend_task1.abort();
    backend_task2.abort();
    proxy.abort();
}
