use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug,Clone)]
pub struct HealthCheckConfig {
    pub kind: HealthCheckKind, // which check to run
    pub interval: Duration, // how often to check
    pub timeout: Duration, // max time per check
    pub healthy_threshold: u32, // how many successes to mark healthy
    pub unhealthy_threshold: u32, // how many failures to mark unhealthy
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            kind: HealthCheckKind::TcpConnect,  // TCP connect works for every backend (even HTTP ones sit on a TCP socket), so it's the safe default
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            healthy_threshold: 2,
            unhealthy_threshold: 3,
        }
    }
}

#[derive(Debug, Clone)]
pub enum HealthCheckKind {
    TcpConnect,
    Http {path: String}
}

async fn tcp_connect_check(addr:SocketAddr, timeout_dur: Duration) -> bool{
    match timeout(timeout_dur, TcpStream::connect(addr) ).await {
        Ok(Ok(_)) => true,
        _ => false
    }
}




pub fn janus_health() -> &'static str {
    "janus-health"
}
