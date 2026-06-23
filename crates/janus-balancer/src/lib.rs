use janus_core::{BackendRuntime, HealthStatus};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
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
        .filter(|c| {
            matches!(
                c.runtime.health(),
                HealthStatus::Healthy | HealthStatus::Unknown
            )
        })
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

#[derive(Default)]
pub struct IpHashBalancer;

impl IpHashBalancer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LoadBalancer for IpHashBalancer {
    fn select(
        &self,
        candidates: &[BackendCandidate],
        ctx: &SelectionContext,
    ) -> Option<SelectedBackend> {
        let healthy = healthy_candidates(candidates);
        if healthy.is_empty() {
            return None;
        }

        if let Some(addr) = ctx.client_addr {
            let mut hasher = DefaultHasher::new();
            addr.ip().hash(&mut hasher);
            let idx = (hasher.finish() as usize) % healthy.len();
            Some(SelectedBackend {
                runtime: Arc::clone(&healthy[idx].runtime),
            })
        } else {
            healthy
                .iter()
                .min_by(|a, b| a.runtime.backend().id.0.cmp(&b.runtime.backend().id.0))
                .map(|candidate| SelectedBackend {
                    runtime: Arc::clone(&candidate.runtime),
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use janus_core::{Backend, BackendAddress, BackendId, BackendRuntime, HealthStatus};
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct TestBackendFixture {
        candidate: BackendCandidate,
        _guards: Vec<janus_core::ActiveConnectionGuard>,
    }

    fn create_fixture(
        id: &str,
        weight: u32,
        health: HealthStatus,
        active_conns: usize,
    ) -> TestBackendFixture {
        let backend = Backend {
            id: BackendId(id.to_string()),
            address: BackendAddress("127.0.0.1:8080".parse().unwrap()),
            weight,
        };
        let runtime = Arc::new(BackendRuntime::new(backend));
        runtime.set_health(health);

        let mut guards = Vec::new();
        for _ in 0..active_conns {
            guards.push(runtime.begin_connection());
        }

        TestBackendFixture {
            candidate: BackendCandidate { runtime },
            _guards: guards,
        }
    }

    #[test]
    fn test_round_robin_balancer() {
        let b1 = create_fixture("b1", 1, HealthStatus::Healthy, 0);
        let b2 = create_fixture("b2", 1, HealthStatus::Healthy, 0);
        let b3 = create_fixture("b3", 1, HealthStatus::Unhealthy, 0);
        let b4 = create_fixture("b4", 1, HealthStatus::Healthy, 0);

        let candidates = vec![
            b1.candidate.clone(),
            b2.candidate.clone(),
            b3.candidate.clone(),
            b4.candidate.clone(),
        ];

        let balancer = RoundRobinBalancer::new();
        let ctx = SelectionContext { client_addr: None };

        let sel1 = balancer.select(&candidates, &ctx).unwrap();
        assert_eq!(sel1.runtime.backend().id.0, "b1");

        let sel2 = balancer.select(&candidates, &ctx).unwrap();
        assert_eq!(sel2.runtime.backend().id.0, "b2");

        let sel3 = balancer.select(&candidates, &ctx).unwrap();
        assert_eq!(sel3.runtime.backend().id.0, "b4");

        let sel4 = balancer.select(&candidates, &ctx).unwrap();
        assert_eq!(sel4.runtime.backend().id.0, "b1");
    }

    #[test]
    fn test_unknown_backends_are_routable_at_startup() {
        // Before the first health probe runs, every backend is Unknown.
        // Traffic must still flow, otherwise the proxy is dead until the
        // first interval tick.
        let b1 = create_fixture("b1", 1, HealthStatus::Unknown, 0);
        let b2 = create_fixture("b2", 1, HealthStatus::Unknown, 0);

        let candidates = vec![b1.candidate.clone(), b2.candidate.clone()];
        let balancer = RoundRobinBalancer::new();
        let ctx = SelectionContext { client_addr: None };

        assert!(balancer.select(&candidates, &ctx).is_some());
    }

    #[test]
    fn test_least_connections_balancer() {
        let b1 = create_fixture("b1", 1, HealthStatus::Healthy, 5);
        let b2 = create_fixture("b2", 1, HealthStatus::Healthy, 2);
        let b3 = create_fixture("b3", 1, HealthStatus::Unhealthy, 1);
        let b4 = create_fixture("b4", 1, HealthStatus::Healthy, 8);

        let candidates = vec![
            b1.candidate.clone(),
            b2.candidate.clone(),
            b3.candidate.clone(),
            b4.candidate.clone(),
        ];

        let balancer = LeastConnectionsBalancer::new();
        let ctx = SelectionContext { client_addr: None };

        let sel = balancer.select(&candidates, &ctx).unwrap();
        assert_eq!(sel.runtime.backend().id.0, "b2");
    }

    #[test]
    fn test_least_connections_tie_breaker() {
        let b1 = create_fixture("b-beta", 1, HealthStatus::Healthy, 2);
        let b2 = create_fixture("b-alpha", 1, HealthStatus::Healthy, 2);

        let candidates = vec![b1.candidate.clone(), b2.candidate.clone()];
        let balancer = LeastConnectionsBalancer::new();
        let ctx = SelectionContext { client_addr: None };

        let sel = balancer.select(&candidates, &ctx).unwrap();
        assert_eq!(sel.runtime.backend().id.0, "b-alpha");
    }

    #[test]
    fn test_weighted_round_robin_balancer() {
        let b1 = create_fixture("b1", 1, HealthStatus::Healthy, 0);
        let b2 = create_fixture("b2", 3, HealthStatus::Healthy, 0);
        let b3 = create_fixture("b3", 2, HealthStatus::Unhealthy, 0);

        let candidates = vec![
            b1.candidate.clone(),
            b2.candidate.clone(),
            b3.candidate.clone(),
        ];

        let balancer = WeightedRoundRobinBalancer::new();
        let ctx = SelectionContext { client_addr: None };

        let mut selections = Vec::new();
        for _ in 0..8 {
            let sel = balancer.select(&candidates, &ctx).unwrap();
            selections.push(sel.runtime.backend().id.0.clone());
        }

        assert_eq!(
            selections,
            vec!["b1", "b2", "b2", "b2", "b1", "b2", "b2", "b2"]
        );
    }

    #[test]
    fn test_ip_hash_balancer() {
        let b1 = create_fixture("b1", 1, HealthStatus::Healthy, 0);
        let b2 = create_fixture("b2", 1, HealthStatus::Healthy, 0);
        let b3 = create_fixture("b3", 1, HealthStatus::Unhealthy, 0);

        let candidates = vec![
            b1.candidate.clone(),
            b2.candidate.clone(),
            b3.candidate.clone(),
        ];

        let balancer = IpHashBalancer::new();

        let addr1: SocketAddr = "192.168.1.100:12345".parse().unwrap();
        let ctx1 = SelectionContext {
            client_addr: Some(addr1),
        };

        let sel1_a = balancer.select(&candidates, &ctx1).unwrap();
        let sel1_b = balancer.select(&candidates, &ctx1).unwrap();
        assert_eq!(sel1_a.runtime.backend().id.0, sel1_b.runtime.backend().id.0);

        let ctx_none = SelectionContext { client_addr: None };
        let sel_fallback = balancer.select(&candidates, &ctx_none).unwrap();
        assert_eq!(sel_fallback.runtime.backend().id.0, "b1");
    }
}
