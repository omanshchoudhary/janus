use crate::config::*;
use janus_core::{Backend, BackendRuntime};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    time::{timeout, Duration},
};

pub(crate) async fn handle_tcp_connection(
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

// Shuts the client stream down, releases the active-connection count, and logs closure.
pub(crate) async fn close_connection(
    client_stream: &mut TcpStream,
    connection_id: u64,
    peer_addr: SocketAddr,
    active_connections: &AtomicUsize,
) {
    let _ = client_stream.shutdown().await;
    let current = decrement_active_connections(active_connections);
    tracing::info!(connection_id, %peer_addr, active_connections = current, "closed connection");
}

pub(crate) async fn connect_backend(
    backend: &Backend,
    timeout_duration: Duration,
) -> janus_core::Result<TcpStream> {
    match timeout(timeout_duration, TcpStream::connect(backend.address.0)).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(error)) => Err(error.into()),
        Err(_) => Err(janus_core::Error::Timeout),
    }
}
