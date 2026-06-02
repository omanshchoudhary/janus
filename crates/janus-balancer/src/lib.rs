use janus_core::{BackendRuntime, HealthStatus};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub fn janus_balancer() -> &'static str {
    "janus-balancer"
}

pub trait LoadBalancer {
    fn select(
        &self,
        candidates: &[BackendCandidate],
        ctx: &SelectionContext,
    ) -> Option<SelectedBackend>;
}

#[derive(Debug, Clone)]
pub struct BackendCandidate {
    pub runtime: Arc<BackendRuntime>,
}

#[derive(Debug, Clone)]
pub struct SelectedBackend {
    pub runtime: Arc<BackendRuntime>,
}

// Include client socket address for future IP-hash algorithms
#[derive(Debug, Clone, Copy)]
pub struct SelectionContext {
    pub client_addr: Option<SocketAddr>,
}

pub fn healthy_candidates(candidates: &[BackendCandidate]) -> Vec<BackendCandidate> {
    candidates
        .iter()
        .filter(|c| c.runtime.health() == HealthStatus::Healthy)
        .cloned()
        .collect()
}

#[derive(Default)]
pub struct RoundRobinBalancer {
    cursor: AtomicUsize,
}

impl RoundRobinBalancer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LoadBalancer for RoundRobinBalancer {
    fn select(
        &self,
        candidates: &[BackendCandidate],
        _ctx: &SelectionContext,
    ) -> Option<SelectedBackend> {
        let healthy = healthy_candidates(candidates);
        if healthy.is_empty() {
            return None;
        }

        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % healthy.len();
        Some(SelectedBackend {
            runtime: Arc::clone(&healthy[idx].runtime),
        })
    }
}
