use std::net::SocketAddr;
// To create mutable shared data for different tasks
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub struct ListenerConfig {
    pub listen_addr: SocketAddr,
}

 pub struct ConnectionId(pub u64);


fn increment_active_connections(active_connections: &AtomicUsize) -> usize {
    // Fetch add returns old value that is why adding plus in return value
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

    loop {
        // Accept Incoming Connection Requests
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
                // Using ARC pointers, pointing to same heap memory by increasing reference counts
                let active_connections_for_task = Arc::clone(&active_connections);
                tokio::spawn(async move {
                    handle_connection(socket, connection_id, addr, active_connections_for_task)
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
    mut socket: TcpStream,
    connection_id: ConnectionId,
    peer_addr: SocketAddr,
    active_connections: Arc<AtomicUsize>,
) {
    let mut buffer = [0_u8; 1024];

    loop {
        match socket.read(&mut buffer).await {
            Ok(0) => {
                // A zero-byte read means the peer closed the connection cleanly.
                break;
            }
            Ok(bytes_read) => {
                if let Err(error) = socket.write_all(&buffer[..bytes_read]).await {
                    tracing::error!(
                        connection_id = connection_id.0,
                        %peer_addr,
                        %error,
                        "failed to write echoed bytes"
                    );
                    break;
                }
            }
            Err(error) => {
                tracing::error!(
                    connection_id = connection_id.0,
                    %peer_addr,
                    %error,
                    "failed to read from client socket"
                );
                break;
            }
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
