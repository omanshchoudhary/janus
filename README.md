# Janus

Janus is a Rust workspace for learning how to build a low-level TCP and HTTP/1.1 reverse proxy and load balancer.

## Project Focus

- Workspace-based Rust architecture.
- Shared core types for backend, protocol, and error handling.
- TCP proxying first, then balancing, health checks, and admin/runtime control.

## Current Progress

Janus currently has the foundation and the first forwarding path in place.

### Completed So Far

- Created the Rust workspace and crate layout.
- Added shared core types in `janus-core`.
- Added CLI config-path parsing in `janus-bin`.
- Added an example config file at `configs/janus.example.toml`.
- Added formatting and lint conventions in `docs/conventions.md` and `.rustfmt.toml`.
- Added a CLI smoke test for `--help`.
- Implemented TCP forwarding with timeout handling.
- Added a TCP forwarding integration test in `janus-proxy`.
- Added connection logging and byte metrics placeholders.

## Workspace Crates

- `janus-core` - shared domain types and errors.
- `janus-config` - config parsing and loading.
- `janus-balancer` - backend selection strategies.
- `janus-health` - health checks and circuit breaker logic.
- `janus-proxy` - TCP proxy/data plane.
- `janus-admin` - admin and metrics API.
- `janus-bin` - executable entrypoint.

## Upcoming Work

- Build the runtime state model for backend counters and health snapshots.
- Add safer connection tracking across all proxy exit paths.
- Start TCP load balancing across multiple backends.
