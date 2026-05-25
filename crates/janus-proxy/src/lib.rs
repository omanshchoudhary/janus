use std::net::SocketAddr;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// To create mutable shared data for different tasks
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

// Our own server configurations
pub struct ListenerConfig {
    pub listen_addr: SocketAddr,
}

pub struct ConnectionId(pub u64);

// Initialize the listener and bind it to a port
pub async fn run_tcp_listener(config: ListenerConfig) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("Listener started on {}", config.listen_addr);
    let mut next_connection_id = 0u64;
    let active_connections = Arc::new(AtomicUsize::new(0));

    loop {
        // Accept Incoming Connection Requests
        match listener.accept().await {
            Ok((socket, addr)) => {
                // Fetch add returns old value that is why adding plus in return value
                let current = active_connections.fetch_add(1, Ordering::Relaxed) + 1;

                next_connection_id += 1;
                let connection_id = ConnectionId(next_connection_id);
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
    // mut socket as reading and writing might change socket state
    let mut buffer = [0_u8; 1024];

    loop {
        match socket.read(&mut buffer).await {
            Ok(0) => {
                break; // EOF
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
    let current = active_connections.fetch_sub(1, Ordering::Relaxed) - 1;
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
