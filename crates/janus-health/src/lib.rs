use janus_core::HealthStatus;
use janus_core::RuntimeState;
use std::{net::SocketAddr, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    pub kind: HealthCheckKind,    // which check to run
    pub interval: Duration,       // how often to check
    pub timeout: Duration,        // max time per check
    pub healthy_threshold: u32,   // how many successes to mark healthy
    pub unhealthy_threshold: u32, // how many failures to mark unhealthy
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            kind: HealthCheckKind::TcpConnect, // TCP connect works for every backend (even HTTP ones sit on a TCP socket), so it's the safe default
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            healthy_threshold: 2,
            unhealthy_threshold: 3,
        }
    }
}

struct HealthTracker {
    status: HealthStatus,
    consecutive_successes: u32,
    consecutive_failures: u32,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
}

impl HealthTracker {
    fn new(config: &HealthCheckConfig) -> Self {
        Self {
            status: HealthStatus::Unknown,
            consecutive_successes: 0,
            consecutive_failures: 0,
            healthy_threshold: config.healthy_threshold,
            unhealthy_threshold: config.unhealthy_threshold,
        }
    }
    fn record(&mut self, passed: bool) -> Option<HealthStatus> {
        if passed {
            self.consecutive_successes += 1;
            self.consecutive_failures = 0;
            if self.consecutive_successes >= self.healthy_threshold
                && self.status != HealthStatus::Healthy
            {
                self.status = HealthStatus::Healthy;
                return Some(self.status);
            }
        } else {
            self.consecutive_failures += 1;
            self.consecutive_successes = 0;
            if self.consecutive_failures >= self.unhealthy_threshold
                && self.status != HealthStatus::Unhealthy
            {
                self.status = HealthStatus::Unhealthy;
                return Some(self.status);
            }
        }
        None
    }
}

#[derive(Debug, Clone)]
pub enum HealthCheckKind {
    TcpConnect,
    Http { path: String },
}

async fn tcp_connect_check(addr: SocketAddr, timeout_dur: Duration) -> bool {
    match timeout(timeout_dur, TcpStream::connect(addr)).await {
        Ok(Ok(_)) => true,
        _ => false,
    }
}

async fn http_check(addr: SocketAddr, path: &str, timeout_dur: Duration) -> bool {
    let result = timeout(timeout_dur, async {
        // connect
        let mut stream = TcpStream::connect(addr).await.ok()?;

        // create a request line
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: healthcheck\r\nConnection: close\r\n\r\n");

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

pub fn spawn_health_supervisor(
    state: RuntimeState,
    config: HealthCheckConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // one tracker per backend, similar to state.backends()
        let mut trackers: Vec<HealthTracker> = state
            .backends()
            .iter()
            .map(|_| HealthTracker::new(&config))
            .collect();
        
        // setting up a periodic background loop using Tokio's timer
        let mut ticker = tokio::time::interval(config.interval);
        loop {
            ticker.tick().await; // Wait for the next scheduled interval before running the health checks again.

            for(backend,tracker) in state.backends().iter().zip(&mut trackers) {
                let addr = backend.backend().address.0;

                let passed = match &config.kind {
                    HealthCheckKind::TcpConnect => tcp_connect_check(addr, config.timeout).await,
                    HealthCheckKind::Http { path } => http_check(addr, path, config.timeout).await,
                };

                if let Some(new_status) = tracker.record(passed) {
                    tracing::info!(backend = %backend.backend().id.0, ?new_status, "health transition");
                    backend.set_health(new_status);
                }
            }
        }
    })
}

pub fn janus_health() -> &'static str {
    "janus-health"
}

#[cfg(test)]
mod tests {
    use super::{HealthCheckConfig, HealthTracker};
    use janus_core::HealthStatus;

    fn config(healthy: u32, unhealthy: u32) -> HealthCheckConfig {
        HealthCheckConfig {
            healthy_threshold: healthy,
            unhealthy_threshold: unhealthy,
            ..Default::default()
        }
    }

    #[test]
    fn failures_below_threshold_do_not_transition() {
        let mut tracker = HealthTracker::new(&config(2, 3));

        // unhealthy_threshold is 3, so the first two failures are not enough.
        assert_eq!(tracker.record(false), None);
        assert_eq!(tracker.record(false), None);
    }

    #[test]
    fn reaching_the_unhealthy_threshold_transitions_once() {
        let mut tracker = HealthTracker::new(&config(2, 3));

        assert_eq!(tracker.record(false), None);
        assert_eq!(tracker.record(false), None);
        // The third consecutive failure crosses the threshold.
        assert_eq!(tracker.record(false), Some(HealthStatus::Unhealthy));
        // Already Unhealthy: further failures must not re-report a transition.
        assert_eq!(tracker.record(false), None);
    }

    #[test]
    fn reaching_the_healthy_threshold_transitions_once() {
        let mut tracker = HealthTracker::new(&config(2, 3));

        assert_eq!(tracker.record(true), None);
        // Second consecutive success crosses healthy_threshold.
        assert_eq!(tracker.record(true), Some(HealthStatus::Healthy));
        assert_eq!(tracker.record(true), None);
    }

    #[test]
    fn a_success_resets_the_failure_streak() {
        let mut tracker = HealthTracker::new(&config(2, 3));

        // Two failures, then a success wipes the failure counter.
        tracker.record(false);
        tracker.record(false);
        tracker.record(true);

        // The streak restarts: two failures are no longer enough to flip.
        assert_eq!(tracker.record(false), None);
        assert_eq!(tracker.record(false), None);
        // Only the third fresh failure crosses the threshold again.
        assert_eq!(tracker.record(false), Some(HealthStatus::Unhealthy));
    }
}
