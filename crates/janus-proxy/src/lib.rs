use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use janus_core::{Backend, BackendAddress, BackendId};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};

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

pub async fn run_tcp_listener(config: ListenerConfig) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("Listener started on {}", config.listen_addr);
    let mut next_connection_id = 0u64;
    let active_connections = Arc::new(AtomicUsize::new(0));
    let backend = Backend {
        id: BackendId("backend-1".to_string()),
        address: BackendAddress("127.0.0.1:9000".parse().expect("valid backend address")),
        weight: 1,
    };

    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
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
                let backend_for_task = backend.clone();
                tokio::spawn(async move {
                    handle_connection(
                        socket,
                        connection_id,
                        addr,
                        active_connections_for_task,
                        backend_for_task,
                    )
                    .await;
                });
            }
            Err(error) => {
                tracing::error!(%error, "failed to accept incoming connection");
            }
        }
    }
}

async fn handle_connection(
    mut client_socket: TcpStream,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    active_connections: Arc<AtomicUsize>,
    backend: Backend,
) {
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
            let duration = started_at.elapsed();
            tracing::info!(
                connection_id = connection_id.0,
                %peer_addr,
                backend_id = %backend.id.0,
                client_to_backend_bytes,
                backend_to_client_bytes,
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
        }
    }

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

#[cfg(test)]
mod tests {
    use super::{decrement_active_connections, increment_active_connections};
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
}
