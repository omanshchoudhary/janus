use janus_balancer::{
    BackendCandidate, LoadBalancer, RoundRobinBalancer, SelectionContext,
};
use janus_core::{
    Backend, BackendAddress, BackendId, BackendRuntime, HealthStatus, RuntimeState,
};
use janus_health::{spawn_health_supervisor, HealthCheckConfig, HealthCheckKind};
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::time::sleep;

fn next_available_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);
    addr
}

fn make_backend(id: &str, addr: SocketAddr) -> Backend {
    Backend {
        id: BackendId(id.to_string()),
        address: BackendAddress(addr),
        weight: 1,
    }
}

/// A backend that accepts and immediately drops every probe connection.
/// Aborting the returned handle drops the listener, which closes the port so
/// later connects are refused — that is how we simulate the backend going down.
fn spawn_backend(addr: SocketAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await.expect("bind backend listener");
        loop {
            let _ = listener.accept().await;
        }
    })
}

fn find(state: &RuntimeState, id: &str) -> Arc<BackendRuntime> {
    state
        .backends()
        .iter()
        .find(|r| r.backend().id.0 == id)
        .cloned()
        .expect("backend should exist in runtime state")
}

async fn wait_for_health(
    runtime: &Arc<BackendRuntime>,
    want: HealthStatus,
    within: Duration,
) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if runtime.health() == want {
            return true;
        }
        sleep(Duration::from_millis(20)).await;
    }
    runtime.health() == want
}

#[tokio::test]
async fn backend_going_down_shifts_traffic_to_the_healthy_one() {
    let addr1 = next_available_addr();
    let addr2 = next_available_addr();
    let backend1 = spawn_backend(addr1);
    let backend2 = spawn_backend(addr2);

    let state = RuntimeState::new(vec![
        BackendRuntime::new(make_backend("b1", addr1)),
        BackendRuntime::new(make_backend("b2", addr2)),
    ]);

    let config = HealthCheckConfig {
        kind: HealthCheckKind::TcpConnect,
        interval: Duration::from_millis(50),
        timeout: Duration::from_millis(200),
        healthy_threshold: 1,
        unhealthy_threshold: 2,
    };
    let supervisor = spawn_health_supervisor(state.clone(), config);

    let rt1 = find(&state, "b1");
    let rt2 = find(&state, "b2");

    // Both backends are up, so both probes succeed and they become Healthy.
    assert!(
        wait_for_health(&rt1, HealthStatus::Healthy, Duration::from_secs(2)).await,
        "b1 should become healthy"
    );
    assert!(
        wait_for_health(&rt2, HealthStatus::Healthy, Duration::from_secs(2)).await,
        "b2 should become healthy"
    );

    // Take b2 down: aborting drops its listener and frees the port.
    backend2.abort();
    let _ = backend2.await;

    // After consecutive failed probes, b2 flips to Unhealthy; b1 stays Healthy.
    assert!(
        wait_for_health(&rt2, HealthStatus::Unhealthy, Duration::from_secs(2)).await,
        "b2 should become unhealthy after going down"
    );
    assert_eq!(rt1.health(), HealthStatus::Healthy, "b1 should stay healthy");

    // Traffic shifts: the balancer now only ever selects b1.
    let candidates: Vec<BackendCandidate> = state
        .backends()
        .iter()
        .map(|runtime| BackendCandidate {
            runtime: Arc::clone(runtime),
        })
        .collect();
    let balancer = RoundRobinBalancer::new();
    let ctx = SelectionContext { client_addr: None };

    for _ in 0..10 {
        let selected = balancer
            .select(&candidates, &ctx)
            .expect("a healthy backend should remain");
        assert_eq!(
            selected.runtime.backend().id.0,
            "b1",
            "traffic should only go to the healthy backend"
        );
    }

    supervisor.abort();
    let _ = supervisor.await;
    backend1.abort();
    let _ = backend1.await;
}
