use janus_balancer::{BackendCandidate, LoadBalancer, RoundRobinBalancer, SelectionContext};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    time::{timeout, Duration},
};

use janus_core::{Backend, BackendRuntime, HealthStatus};
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

// Bind our listener to a port.
pub async fn run_tcp_listener(config: ListenerConfig, backend: Backend) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("Listener started");
    serve_tcp_listener(listener, backend).await
}

pub async fn serve_tcp_listener(listener: TcpListener, backend: Backend) -> janus_core::Result<()> {
    let balancer = Arc::new(RoundRobinBalancer::new());
    serve_tcp_listener_multi(listener, vec![backend], balancer).await
}

pub async fn run_tcp_listener_multi(
    config: ListenerConfig,
    backends: Vec<Backend>,
    balancer: Arc<dyn LoadBalancer + Send + Sync>,
) -> janus_core::Result<()> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    tracing::info!("Listener started");
    serve_tcp_listener_multi(listener, backends, balancer).await
}

pub async fn serve_tcp_listener_multi(
    listener: TcpListener,
    backends: Vec<Backend>,
    balancer: Arc<dyn LoadBalancer + Send + Sync>,
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
                    handle_connection(
                        socket,
                        connection_id,
                        addr,
                        active_connections_for_task,
                        backend_runtime_for_task,
                        metrics_for_task,
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
