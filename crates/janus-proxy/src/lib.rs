use std::net::SocketAddr;
use tokio::net::TcpListener;

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

    loop {
        match listener.accept().await {
            Ok((_socket, addr)) => {
                next_connection_id += 1;
                let connection_id = ConnectionId(next_connection_id);
                tokio::spawn(async move {
                    handle_connection(connection_id, addr).await;
                });
            }
            Err(error) => {
                tracing::error!(%error, "failed to accept incoming connection");
            }
        }
    }
}
async fn handle_connection(connection_id: ConnectionId, peer_addr: SocketAddr) {
    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        "handling incoming connection"
    );
    tracing::info!(
        connection_id = connection_id.0,
        %peer_addr,
        "closed connection"
    );
}
pub fn janus_proxy() -> &'static str {
    "janus-proxy"
}
