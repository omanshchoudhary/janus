#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::{net::SocketAddr, sync::Arc};

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

#[derive(Debug)]
pub struct BackendRuntime;

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
}

pub fn janus_core() -> &'static str {
    "janus-core"
}
