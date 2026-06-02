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

#[derive(Default)]
pub struct LeastConnectionsBalancer;

impl LeastConnectionsBalancer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LoadBalancer for LeastConnectionsBalancer {
    fn select(
        &self,
        candidates: &[BackendCandidate],
        _ctx: &SelectionContext,
    ) -> Option<SelectedBackend> {
        let healthy = healthy_candidates(candidates);
        if healthy.is_empty() {
            return None;
        }

        let chosen = healthy.into_iter().min_by(|a, b| {
            let a_load = a.runtime.active_connections();
            let b_load = b.runtime.active_connections();
            a_load
                .cmp(&b_load)
                .then_with(|| a.runtime.backend().id.0.cmp(&b.runtime.backend().id.0))
        });

        chosen.map(|candidate| SelectedBackend {
            runtime: Arc::clone(&candidate.runtime),
        })
    }
}

#[derive(Debug, Default)]
pub struct WeightedRoundRobinBalancer {
    cursor: AtomicUsize,
}

impl WeightedRoundRobinBalancer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LoadBalancer for WeightedRoundRobinBalancer {
    fn select(
        &self,
        candidates: &[BackendCandidate],
        _ctx: &SelectionContext,
    ) -> Option<SelectedBackend> {
        let healthy = healthy_candidates(candidates);
        if healthy.is_empty() {
            return None;
        }

        let expanded = expanded_indexes(&healthy);
        let slot;
        let idx;
        if expanded.is_empty() {
            idx = self.cursor.fetch_add(1, Ordering::Relaxed) % healthy.len();
        } else {
            slot = self.cursor.fetch_add(1, Ordering::Relaxed) % expanded.len();
            idx = expanded[slot];
        }

        Some(SelectedBackend {
            runtime: Arc::clone(&healthy[idx].runtime),
        })
    }
}

fn expanded_indexes(candidates: &[BackendCandidate]) -> Vec<usize> {
    let mut expanded = Vec::new();

    for (idx, candidate) in candidates.iter().enumerate() {
        let weight = candidate.runtime.backend().weight;
        for _ in 0..weight {
            expanded.push(idx);
        }
    }
    expanded
}
