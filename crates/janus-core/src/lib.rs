#![deny(clippy::all)]
#![warn(clippy::pedantic)]

use std::net::SocketAddr;

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
pub type BackendId = u64;

#[derive(Debug, Clone)]
pub struct Backend {
    pub id: BackendId,
    pub address: SocketAddr,
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

pub fn janus_core() -> &'static str {
    "janus-core"
}
