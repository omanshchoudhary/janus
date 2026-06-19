use std::{net::SocketAddr, time::Duration};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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

async fn http_check(addr: SocketAddr, path: &str, timeout_dur: Duration) -> bool {
    let result = timeout(timeout_dur, async {
        
        // connect
        let mut stream = TcpStream::connect(addr).await.ok()?;

        // create a request line
        let request = format!(
             "GET {path} HTTP/1.1\r\nHost: healthcheck\r\nConnection: close\r\n\r\n"
        );

        // sending request
        stream.write_all(request.as_bytes()).await.ok()?;

        let mut reader = BufReader::new(stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line).await.ok()?;

        let code = status_line.split_whitespace().nth(1)?.parse::<u16>().ok()?;
        Some((200..=299).contains(&code))
    }) 
    .await;

    matches!(result, Ok(Some(true)))
}


pub fn janus_health() -> &'static str {
    "janus-health"
}
