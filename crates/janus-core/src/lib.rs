#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

// Error Types
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("No backend available")]
    BackendUnavailable,

    #[error("Operation timed out")]
    Timeout,

    #[error("Invalid state: {0}")]
    InvalidState(String),
}

// Result Alias
pub type Result<T> = std::result::Result<T, Error>;

// Protocol Enums
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Http1,
}

// Backend Identity
#[derive(Debug, Clone)]
pub struct BackendId(pub String);

// Backend Address
#[derive(Debug, Clone)]
pub struct BackendAddress(pub SocketAddr);

// Backend Config
#[derive(Debug, Clone)]
pub struct Backend {
    pub id: BackendId,
    pub address: BackendAddress,
    pub weight: u32,
}

// Health Status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    Unknown,
    Healthy,
    Unhealthy,
    Draining,
}

// Stores the runtime state and health metrics of a backend
#[derive(Debug)]
pub struct BackendRuntime {
    backend: Backend,
    active_connections: AtomicUsize,
    total_connections: AtomicU64,
    total_failures: AtomicU64,
    health: RwLock<HealthStatus>,
}

impl BackendRuntime {
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            active_connections: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            health: RwLock::new(HealthStatus::Unknown),
        }
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    pub fn total_connections(&self) -> u64 {
        self.total_connections.load(Ordering::Relaxed)
    }

    pub fn total_failures(&self) -> u64 {
        self.total_failures.load(Ordering::Relaxed)
    }

    pub fn health(&self) -> HealthStatus {
        *self
            .health
            .read()
            .expect("Failed to acquire read lock on backend health status")
    }

    pub fn set_health(&self, status: HealthStatus) {
        *self
            .health
            .write()
            .expect("Failed to acquire write lock on backend health status") = status;
    }

    pub fn snapshot(&self) -> BackendSnapshot {
        BackendSnapshot {
            id: self.backend.id.clone(),
            address: self.backend.address.clone(),
            weight: self.backend.weight,
            health: self.health(),
            active_connections: self.active_connections(),
            total_connections: self.total_connections(),
            total_failures: self.total_failures(),
        }
    }
    pub fn begin_connection(self: &Arc<Self>) -> ActiveConnectionGuard {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        ActiveConnectionGuard {
            backend: Arc::clone(self),
        }
    }

    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::Relaxed);
    }
}

// Runtime state for backend live status
#[derive(Clone, Debug)]
pub struct RuntimeState {
    backends: Arc<Vec<Arc<BackendRuntime>>>,
}

impl RuntimeState {
    pub fn new(backends: Vec<BackendRuntime>) -> Self {
        let backends = backends.into_iter().map(Arc::new).collect::<Vec<_>>();
        Self {
            backends: Arc::new(backends),
        }
    }

    pub fn backends(&self) -> &[Arc<BackendRuntime>] {
        self.backends.as_ref().as_slice()
    }

    pub fn snapshots(&self) -> Vec<BackendSnapshot> {
        self.backends.iter().map(|b| b.snapshot()).collect()
    }
}

// Immutable view of runtime state
#[derive(Clone, Debug)]
pub struct BackendSnapshot {
    pub id: BackendId,
    pub address: BackendAddress,
    pub weight: u32,
    pub health: HealthStatus,
    pub active_connections: usize,
    pub total_connections: u64,
    pub total_failures: u64,
}

pub struct ActiveConnectionGuard {
    backend: Arc<BackendRuntime>,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        self.backend
            .active_connections
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_backend() -> Backend {
        Backend {
            id: BackendId("backend-1".to_string()),
            address: BackendAddress("127.0.0.1:9000".parse().expect("valid socket address")),
            weight: 1,
        }
    }

    #[test]
    fn active_connection_guard_decrements_on_drop() {
        let runtime = Arc::new(BackendRuntime::new(sample_backend()));

        assert_eq!(runtime.active_connections(), 0);
        assert_eq!(runtime.total_connections(), 0);

        {
            let _guard = runtime.begin_connection();
            assert_eq!(runtime.active_connections(), 1);
            assert_eq!(runtime.total_connections(), 1);
        }

        assert_eq!(runtime.active_connections(), 0);
        assert_eq!(runtime.total_connections(), 1);
    }
}

pub fn janus_core() -> &'static str {
    "janus-core"
}
