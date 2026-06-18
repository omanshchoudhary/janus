# Janus

Janus is a Rust workspace for learning how to build a low-level TCP and HTTP/1.1 reverse proxy and load balancer.

## Project Focus

- Workspace-based Rust architecture.
- Shared core types for backend, protocol, and error handling.
- TCP proxying first, then balancing, health checks, and admin/runtime control.

## Current Status

Janus has a working TCP data plane and an HTTP/1.1 reverse proxy. It accepts
client connections, selects a backend through a pluggable load-balancing
strategy, and forwards traffic while tracking per-backend runtime state. On the
HTTP path it parses the request, rewrites headers, forwards the request to the
selected backend, and relays the response back to the client over a single
request/response exchange.

### Implemented

- Shared domain types in `janus-core`: `Backend`, `Protocol`, `HealthStatus`,
  and the common `Error` and `Result` types.
- Runtime state model in `janus-core`: `BackendRuntime` with atomic active,
  total, and failure counters, along with `RuntimeState`, `BackendSnapshot`,
  and an RAII `ActiveConnectionGuard` that releases the active-connection count
  on drop.
- Four load-balancing strategies in `janus-balancer` behind a `LoadBalancer`
  trait, each restricted to healthy backends:
  - Round Robin (atomic cursor).
  - Least Connections (deterministic identifier tie-breaker).
  - Weighted Round Robin (expanded-index with a zero-weight fallback).
  - IP Hash (deterministic fallback when no client address is present).
- TCP forwarding in `janus-proxy` using `tokio::io::copy_bidirectional`, with
  connect timeouts, multi-backend support, balancer-driven selection,
  connection logging, and bytes-in/bytes-out metric counters.
- A per-service protocol mode (`tcp` or `http1`) selected at the listener,
  reusing the same balancing engine for both paths.
- An HTTP/1.1 reverse proxy in `janus-proxy`: reading the request head from a
  buffered stream, parsing the request line and headers, origin-form target
  validation, `Content-Length` parsing, rejection of unsupported requests
  (chunked bodies and protocol upgrades), hop-by-hop header stripping, and
  `X-Forwarded-For`/`-Host`/`-Proto` injection. The request and its body are
  forwarded to the selected backend, and the parsed response head and body are
  relayed back to the client, with response status-class counters
  (`1xx`–`5xx`). Connections close after a single exchange; a client keep-alive
  loop is not yet implemented.
- A TOML configuration loader in `janus-config`.
- Config-path argument parsing in `janus-bin`, with a `--help` smoke test.
- An example configuration at `configs/janus.example.toml` and formatting and
  lint conventions in `docs/conventions.md` and `.rustfmt.toml`.
- Unit tests for the balancing algorithms, connection counters, and HTTP
  request/response parsing, plus TCP echo, load-balancing, and HTTP
  reverse-proxy integration tests in `janus-proxy`.

## Workspace Crates

| Crate | Responsibility | Status |
| --- | --- | --- |
| `janus-core` | Shared domain types, errors, and runtime state | Implemented |
| `janus-config` | Configuration parsing and loading | Minimal loader; full schema pending |
| `janus-balancer` | Backend selection strategies | All four strategies implemented |
| `janus-health` | Health checks and circuit breaker logic | Not started |
| `janus-proxy` | TCP proxy and HTTP/1.1 reverse proxy data plane | TCP forwarding, balancing, and the HTTP/1.1 reverse proxy implemented |
| `janus-admin` | Admin and metrics API | Not started |
| `janus-bin` | Executable entrypoint | Argument parsing only; runtime wiring pending |

## Known Limitations

- `janus-config` does not yet parse the full schema used in
  `configs/janus.example.toml`. Protocol, balancing strategy, timeouts,
  retries, and health-check sections are not modeled.
- `janus-bin` parses a config path but does not yet assemble services and
  listeners from the loaded configuration.

## Next Steps

- Add a client keep-alive loop so a connection can carry multiple HTTP
  requests instead of closing after one exchange.
- Expand the `janus-config` schema to match the example configuration and wire
  it into `janus-bin` so services start from file.
- Add active health checks and health-aware routing in `janus-health`.
