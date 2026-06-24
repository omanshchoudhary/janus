use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

// Bytes-in and bytes-out metrics placeholders for overall proxy
pub struct ProxyMetrics {
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub responses_1xx: AtomicU64,
    pub responses_2xx: AtomicU64,
    pub responses_3xx: AtomicU64,
    pub responses_4xx: AtomicU64,
    pub responses_5xx: AtomicU64,
}

impl ProxyMetrics {
    pub fn record_status(&self, status: u16) {
        let counter = match status / 100 {
            1 => &self.responses_1xx,
            2 => &self.responses_2xx,
            3 => &self.responses_3xx,
            4 => &self.responses_4xx,
            _ => &self.responses_5xx,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

// Janus server's listening address.
pub struct ListenerConfig {
    pub listen_addr: SocketAddr,
}

// Unique id for a single client connection to the proxy
pub struct ConnectionId(pub u64);

#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(60),
            idle_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32, // includes the first attempt
    pub retry_on_connect_failure: bool,
    pub retry_on_status: Vec<u16>,      // e.g. [502, 503, 504]
    pub backoff: Duration,              // wait before another retry
    pub retryable_methods: Vec<String>, // methods safe to retry (idempotent by default)
}
impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 2,
            retry_on_connect_failure: true,
            retry_on_status: vec![502, 503, 504],
            backoff: Duration::from_millis(100),
            retryable_methods: vec![
                "GET".into(),
                "HEAD".into(),
                "PUT".into(),
                "DELETE".into(),
                "OPTIONS".into(),
                "TRACE".into(),
            ],
        }
    }
}
pub fn increment_active_connections(active_connections: &AtomicUsize) -> usize {
    active_connections.fetch_add(1, Ordering::Relaxed) + 1
}

pub fn decrement_active_connections(active_connections: &AtomicUsize) -> usize {
    active_connections.fetch_sub(1, Ordering::Relaxed) - 1
}
