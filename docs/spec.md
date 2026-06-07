# Sparkl Router — Technical Specification

## Overview

`sparkl-router` is a high-performance Rust service deployed at `api.sparkl.network`. It provides two surfaces:

1. **Consumer surface** — a public HTTPS/SSE endpoint exposing OpenAI-compatible `/v1/*` routes
2. **Node surface** — a `wss://` endpoint over which inference nodes subscribe (WSS) and receive forwarded requests

Nodes connect outbound via WebSocket, requiring no TLS cert, no open inbound port, and no static IP. The router owns the only TLS certificate in the system. All session authorisation is verified against the `SettlementEscrow` smart contract before any request reaches a node.

***

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        api.sparkl.network                            │
│                                                                      │
│  ┌─────────────────┐    ┌──────────────────────────────────────┐    │
│  │  Consumer Layer  │    │           Node Tunnel Layer          │    │
│  │  HTTPS + SSE     │    │           WSS (outbound from nodes)  │    │
│  │                  │    │                                      │    │
│  │ POST /v1/chat/.. │    │  /node/connect  (WSS upgrade)        │    │
│  │ GET  /v1/models  │    │  per-node persistent connection      │    │
│  │ POST /sessions/  │    │  TunnelRegistry: DashMap<nodeId,Tx>  │    │
│  │      :id/activate│    │                                      │    │
│  └────────┬─────────┘    └──────────────────────────────────────┘    │
│           │                           │                              │
│  ┌────────▼───────────────────────────▼──────────────────────────┐  │
│  │                     Router Core (Tokio/Axum)                   │  │
│  │  AuthMiddleware → ChainVerifier → TunnelDispatcher → SSEBridge │  │
│  └───────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
         │                                        │
         │ HTTPS/SSE                              │ WSS (outbound)
         ▼                                        ▼
    Consumer client                          sparkl-solo node
    (browser, curl, SDK)                     (plain HTTP internally)
```

***

## Technology Stack

| Component | Choice | Rationale |
|---|---|---|
| Runtime | `tokio` (multi-threaded) | Zero GC pauses, handles thousands of concurrent connections[^1][^2] |
| HTTP framework | `axum 0.7` | Native WSS upgrade, SSE, middleware tower layers, shares codebase with sparkl-solo[^3][^1] |
| WebSocket | `tokio-tungstenite` | Most downloaded, best maintained, direct tokio integration[^1] |
| TLS | `rustls` via `axum-server` | Memory-safe, no OpenSSL dependency, ACME/Let's Encrypt integration |
| Tunnel registry | `DashMap<Bytes32, NodeTunnel>` | Lock-free concurrent hashmap, no contention on hot path[^4] |
| Chain reads | `viem`-equivalent: `alloy-rs` | Async EVM reads, connection pooled to RPC endpoint |
| Serialisation | `serde_json` | Standard, zero-copy where possible |
| Metrics | `prometheus` via `metrics` crate | Per-node request counts, latency histograms, tunnel uptime |

***

## Request Multiplexing Over a Single WSS Connection

The central engineering challenge is that a single node holds **one WSS connection** to the router, but many concurrent consumers may be sending inference requests to that node simultaneously. Each request must be matched to its response without interleaving.[^5][^6]

The solution is a **request ID envelope** protocol layered over the WebSocket messages:

```
Router ──► Node:  { "rid": "<uuid>", "type": "request",  "method": "POST", "path": "/v1/chat/completions", "headers": {...}, "body": "..." }
Node   ──► Router: { "rid": "<uuid>", "type": "chunk",    "data": "data: {\"choices\":[...]}\n\n" }
Node   ──► Router: { "rid": "<uuid>", "type": "chunk",    "data": "data: {\"choices\":[...]}\n\n" }
Node   ──► Router: { "rid": "<uuid>", "type": "end",      "status": 200 }
```

The router maintains a `PendingMap: DashMap<Uuid, oneshot::Sender<RouterFrame>>` or `mpsc::Sender<RouterFrame>` per in-flight request. On receiving a `chunk` or `end` frame, it looks up the `rid` and routes the data to the correct consumer SSE stream. This is identical in structure to the multiplexing used by ngrok and similar tunnel services.[^6][^7][^5]

```rust
// Core tunnel state per connected node
pub struct NodeTunnel {
    pub node_id: [u8; 32],
    pub sender: mpsc::Sender<RouterFrame>,       // router → node
    pub pending: Arc<DashMap<Uuid, mpsc::Sender<RouterFrame>>>,  // rid → consumer
    pub connected_at: Instant,
    pub last_ping: Arc<AtomicI64>,
}

// Shared router state
pub struct RouterState {
    pub tunnels: Arc<DashMap<[u8; 32], NodeTunnel>>,
    pub chain: Arc<ChainVerifier>,
}
```

Each node tunnel spawns two tokio tasks on connect:
- **Reader task** — receives frames from the node WSS, routes `chunk`/`end` to pending map
- **Writer task** — receives `RouterFrame` from an `mpsc::Receiver`, writes to node WSS

This avoids any `Mutex` on the write path — the `mpsc::Sender` is `Clone + Send` and requires no locking.[^8][^4]

***

## Node Registration Protocol (`/node/connect`)

Nodes connect once on startup and maintain a persistent WSS connection.

### Handshake sequence

```
Node → Router:  WSS upgrade to wss://api.sparkl.network/node/connect
Router → Node:  { "type": "challenge", "nonce": "<32-byte-hex>", "block": 12345678 }
Node → Router:  { "type": "auth", "node_id": "0x<bytes32>", "signature": "0x<ed25519-sig-over-challenge>" }
Router:         1. ecrecover / ed25519 verify: pubkey → node_id matches
                2. call registry.nodeOperator(node_id) → must be non-zero (registered)
                3. insert into TunnelRegistry
Router → Node:  { "type": "ready", "router_url": "https://api.sparkl.network" }
```

The signature covers `keccak256("sparkl-router-connect:" || nonce || block_number)` — matching the pattern established by the `/identity` endpoint in sparkl-solo. MVP does not enforce a block replay window; the router manages trust via signature verification, on-chain commercial registration (`nodeOperator`), and tunnel lifecycle (ping/pong, catalog liveness).

### Reconnection

Nodes must implement exponential backoff with jitter on disconnect. The router does not persist tunnel state — reconnection is a clean re-auth. In-flight requests that were pending at disconnect receive a `502 Bad Gateway` response to the consumer.[^1]

### Keepalive

Router sends `{ "type": "ping" }` every 30 seconds. Node must respond `{ "type": "pong" }` within 10 seconds or the connection is closed and removed from `TunnelRegistry`.

***

## Consumer Authentication

Every request to `/v1/*` must carry `Authorization: Bearer sk_<base58(sessionId[^32] || secret[^32])>`.

### Middleware pipeline

```
Request
  │
  ▼
BearerParser
  │  decode base58 → split first 32 bytes (sessionId) / last 32 bytes (secret)
  │  reject 400 if malformed
  ▼
ChainVerifier  (cached, TTL 12s = ~1 block)
  │  read escrow.sessions(sessionId) → { user, nodeId, state, modelId }
  │  reject 401 if state != Open
  │  reject 404 if nodeId not in TunnelRegistry
  ▼
TunnelLookup
  │  tunnels.get(nodeId) → NodeTunnel
  │  reject 503 if node offline
  ▼
RequestForwarder + SSEBridge
  │  allocate rid, insert pending mpsc channel
  │  send RouterFrame to node via tunnel.sender
  │  stream chunks back to consumer as SSE
  ▼
Consumer receives SSE stream
```

The `secret` bytes in the bearer token are **not verified by the router**. The router only uses `sessionId` (public on-chain data) to find the correct node, then forwards the full request including the `Authorization` header to the node. The node re-derives `HMAC(nodeKey, sessionId || userAddress)` and verifies the secret itself. This means the router cannot forge valid bearer tokens even though it sees them in transit — the HMAC key never leaves the node.

### Chain verification cache

EVM reads are expensive. A `moka` or `quick_cache` TTL cache keyed on `sessionId` holds verified session state for 12 seconds (one Paseo block). Settled or closed sessions are evicted immediately on `SessionClosed` event subscription.

***

## Session Activation (`/sessions/:sessionId/activate`)

```
POST /sessions/:sessionId/activate
Content-Type: application/json
{ "signature": "0x...", "blockNumber": "12345678" }
```

This endpoint proxies the activation request to the correct node tunnel:

1. Parse `sessionId` from path
2. Read `escrow.sessions(sessionId)` → get `nodeId`
3. Lookup `tunnels.get(nodeId)` → get tunnel
4. Forward as `{ "type": "activate_request", "rid": uuid, "session_id": sessionId, "signature": sig, "block_number": bn }`
5. Node derives HMAC secret, responds `{ "type": "activate_response", "rid": uuid, "api_key": "sk_..." }`
6. Router forwards `{ "apiKey": "sk_..." }` to consumer

The router sees the `apiKey` in transit — this is the low-trust exposure noted in the architecture discussion. The consumer-facing portal displays it in a one-time modal and drops it from memory on close.

***

## Node-Side Changes (`sparkl-solo`)

### New config section

```toml
[router]
enabled = true
url = "wss://api.sparkl.network/node/connect"
reconnect_interval_secs = 5
reconnect_max_secs = 60
```

### New module: `src/router_client.rs`

```rust
pub async fn run_router_client(state: AppState) {
    loop {
        match connect_and_serve(&state).await {
            Ok(()) => break,  // clean shutdown
            Err(e) => {
                warn!("Router connection lost: {e}, reconnecting...");
                // exponential backoff with jitter
                sleep(backoff.next()).await;
            }
        }
    }
}

async fn connect_and_serve(state: &AppState) -> Result<()> {
    let (ws, _) = connect_async(&state.config.router.url).await?;
    let (mut write, mut read) = ws.split();

    // 1. Receive challenge
    // 2. Sign with node Ed25519 key
    // 3. Send auth frame
    // 4. Receive ready

    // Serve loop: receive RouterFrame, dispatch to existing HTTP handlers
    while let Some(msg) = read.next().await {
        let frame: RouterFrame = serde_json::from_str(&msg?.into_text()?)?;
        match frame.frame_type.as_str() {
            "request" => {
                tokio::spawn(handle_tunnelled_request(frame, write_tx.clone(), state.clone()));
            }
            "ping" => { write.send(pong_frame()).await?; }
            _ => {}
        }
    }
    Ok(())
}
```

The `handle_tunnelled_request` function reconstructs an `axum::Request` from the frame and calls into the existing handler functions directly — no duplication of business logic.[^3]

### New endpoint: `POST /sessions/:sessionId/activate`

Already specified above. Returns `{ "apiKey": "sk_<base58>" }` once, with no storage.

***

## Wire Protocol Reference

All frames are JSON objects sent as WebSocket `Text` messages.

### Router → Node frames

| `type` | Fields | Description |
|---|---|---|
| `challenge` | `nonce: hex`, `block: u64` | Auth challenge on connect |
| `ready` | `router_url: string` | Auth accepted |
| `request` | `rid: uuid`, `method`, `path`, `headers: object`, `body: string\|null` | HTTP request to forward |
| `activate_request` | `rid: uuid`, `session_id: hex`, `signature: hex`, `block_number: u64` | Session activation |
| `ping` | — | Keepalive |

### Node → Router frames

| `type` | Fields | Description |
|---|---|---|
| `auth` | `node_id: hex`, `signature: hex`, `ed25519_pubkey: hex` (required), `moniker: string` (optional, max 128 chars) | Auth response to challenge |
| `pong` | — | Keepalive response |
| `response` | `rid: uuid`, `status: u16`, `headers: object` | HTTP response status/headers for both streaming and non-streaming forwards |
| `chunk` | `rid: uuid`, `data: string` | Body segment: SSE line(s) for streaming, raw body bytes (UTF-8 text) for non-streaming |
| `end` | `rid: uuid`, `status: u16` | Terminal frame for request completion (streaming or non-streaming) |
| `error` | `rid: uuid`, `code: u16`, `message: string` | Handler error |
| `activate_response` | `rid: uuid`, `api_key: string` | Session activation result |

***

## Consumer API Surface

All endpoints are OpenAI-compatible. No changes needed to existing client SDKs.

### `POST /v1/chat/completions`

Standard OpenAI chat completions. The node always returns tunnel frames as `response -> chunk* -> end`: when `"stream": true`, `chunk` payloads contain verbatim SSE lines (`data: ...\n\n`); when `"stream": false`, `chunk` payloads contain raw response body segments that the router concatenates into a normal JSON HTTP response.[^9][^10]

### `GET /v1/models`

Aggregated model list across all connected nodes. The router queries each connected node's model list (cached, refreshed on node connect and on throttled WSS `pong` heartbeats) and merges results by model ID.

Each node's `/v1/models` entries may include OpenAI fields plus a `sparkl` object (e.g. `quantization`, `parameter_count`, `source_url`, `concurrency`, `active_sessions`, `context_length` on the parent object). When the same model ID is offered by multiple nodes, the router keeps static fields from the first seen entry and adds:

- `sparkl.providers`: per-node rows with `node_id`, static metadata, `features` (key → freeform value), `active_sessions`, `concurrency`, `available_slots`
- `sparkl.active_sessions`: sum across providers
- `sparkl.available_slots`: sum of `max(0, concurrency - active_sessions)` per provider (when concurrency is set)
- `sparkl.features`: map on each model object when solo config declares features

Response shape: OpenAI-compatible `{ "object": "list", "data": [...] }` with full model objects, not id-only stubs.

### `GET /v1/catalog/features`

Public. Returns the allowed feature **keys** and operator descriptions for solo `[[models]].features` (not live provider values).

```json
{ "object": "feature_catalog", "data": [{ "key": "mtp", "description": "..." }] }
```

### `GET /v1/catalog/providers`

Public, filterable **provider-centric** discovery (which nodes offer a model with spare capacity).

Query parameters: `model`, `features_any`, `features_all` (comma-separated keys), `feature_<key>` (substring match on that key’s value), `quantization`, `parameter_count`, `min_context_length`, `min_available_slots`, `online_only` (default `true`).

Response: `{ "object": "provider_list", "data": [{ "node_id", "model_id", "tunnel_status", "features", "available_slots", ... }] }`.

Use `GET /v1/models` for OpenAI-style model browsing; use `/v1/catalog/providers` for routing/discovery queries.

### `POST /sessions/:sessionId/activate`

Session key activation. Described above. Returns `{ "apiKey": "sk_...", "sessionId": "0x..." }`.

### `GET /health`

Returns `{ "status": "ok", "tunnels": N, "uptime_secs": N }`. No auth required.

### `GET /metrics`

Prometheus metrics endpoint. Restricted to internal network / admin token.

***

## Performance Characteristics

The architecture is designed for high concurrency with minimal contention.[^11][^1]

- **One tokio task per in-flight request** — tasks are cheap (128–256 bytes stack by default in tokio), thousands concurrent is routine[^2]
- **DashMap for tunnel registry** — sharded concurrent hashmap, no global lock on the hot path[^4]
- **mpsc channels for write serialisation** — avoids `Mutex<WebSocket>` which would serialize all writes to a node[^8]
- **Chain verification cache** — eliminates redundant RPC calls; one EVM read per 12 seconds per active session
- **Zero-copy SSE bridging** — `chunk` frames contain verbatim SSE lines; the router writes them directly to the consumer response body without re-serialising
- **No heap allocation on the forward path** — frame routing is pointer/channel passing; no cloning of body bytes where avoidable

Expected capacity on a single 4-core VPS: 10,000+ concurrent consumer SSE streams, 500+ connected node tunnels, sub-millisecond routing overhead per chunk.[^1][^11]

***

## Deployment

### Repository

`sparkl-network/sparkl-router` — new repo, Rust workspace, single binary.

### Runtime requirements

- Single VPS, 2–4 cores, 2 GB RAM (can serve thousands of concurrent connections)[^2]
- Caddy as TLS termination front-end (HTTPS → HTTP to Axum, WSS → WS to Axum)
- No database — all state is in-memory; node tunnels reconnect on restart

### Caddyfile

```
api.sparkl.network {
    reverse_proxy localhost:3001
}
```

Caddy handles Let's Encrypt issuance and renewal automatically. The Axum router binds plain HTTP on `localhost:3001`.

### Environment / config

```toml
[server]
bind = "127.0.0.1:3001"
upstream_timeout_secs = 120

[chain]
rpc_url = "https://paseo-rpc.dwellir.com"
registry_contract = "0x..."
escrow_contract = "0x..."
session_cache_ttl_secs = 12

[node_auth]
ping_interval_secs = 30
pong_timeout_secs = 10

[metrics]
bind = "127.0.0.1:9091"
```

***

## Security Properties

| Property | Mechanism |
|---|---|
| Node identity | Ed25519 signature over router challenge; verified against on-chain `nodeOperator` |
| Consumer session validity | On-chain `escrow.sessions(sessionId).state == Open` verified before forwarding |
| Secret confidentiality | Bearer secret verified by node HMAC; router cannot forge valid tokens |
| Replay prevention | Challenge nonce + block number window; session cache evicted on `SessionClosed` events |
| Node cannot serve unregistered traffic | Router only forwards to nodes present in on-chain registry |
| DoS surface | Rate limiting per IP on consumer endpoints; nodes authenticated before entering registry |

The router operates in **low-trust mode** — it sees request bodies and bearer tokens in transit. Full zero-trust (end-to-end Noise encryption between consumer and node) is a future upgrade path and does not require protocol changes at the consumer API layer.

***

## Build Order

| Phase | Work | Estimated effort |
|---|---|---|
| 1 | `sparkl-router`: tunnel registry, node WSS connect/auth, keepalive | 3–4 days |
| 2 | `sparkl-router`: consumer HTTPS layer, bearer parsing, chain verifier | 2–3 days |
| 3 | `sparkl-router`: request forwarder, SSE bridge, multiplexed pending map | 2–3 days |
| 4 | `sparkl-solo`: `router_client.rs`, tunnel frame dispatch, `/sessions/:id/activate` | 2 days |
| 5 | Integration test: local router + local node + local consumer | 1 day |
| 6 | Deploy to VPS, Caddy TLS, Paseo testnet smoke test | 1 day |

Total: approximately 11–14 days to a working testnet deployment.

---

## References

1. [Rust WebSocket Guide: tokio-tungstenite, axum & JoinSet](https://websocket.org/guides/languages/rust/) - Use tokio-tungstenite for async WebSocket clients and servers. For web apps, axum has built-in WebSo...

2. [How to Build a Scalable WebSocket Server with Tokio in Rust](https://oneuptime.com/blog/post/2026-01-25-scalable-websocket-server-tokio-rust/view) - Learn how to build a production-ready WebSocket server in Rust using Tokio and the tungstenite libra...

3. [Create a WebSocket Server with Axum - MojoAuth](https://mojoauth.com/websocket/create-a-websocket-server-with-axum) - Build a real-time WebSocket server with Axum. Learn practical steps for handling connections and mes...

4. [Rust + WebSockets: Handling Real-Time Events Without Data Loss](https://www.linkedin.com/pulse/rust-websockets-handling-real-time-events-without-data-rajendren-2cwfc) - With the right patterns (tokio, channels, Arc<Mutex>), you can confidently handle real-time events w...

5. [The BEAM and the Crab: Building Tunnels - Adrián Carayol Orenes](https://acaor.com/posts/the-beam-and-the-crab/) - We explore the challenges of multiplexing HTTP requests over a single WebSocket connection, the eleg...

6. [ngrok alternative for Node.js in 600 LOC and no dependencies](https://www.reddit.com/r/programming/comments/1g42bjk/h2tunnel_ngrok_alternative_for_nodejs_in_600_loc/) - The client listens for an HTTP2 connection on the socket from which it initiated the TLS tunnel. The...

7. [Introducing end-to-end HTTP/2 support from client to origin server](https://ngrok.com/blog/http2-support) - The multiplexing and stream prioritization allow for multiple channels that can run in parallel and ...

8. [Axum state, websockets, and Sync error - help - Rust Users Forum](https://users.rust-lang.org/t/axum-state-websockets-and-sync-error/94834) - The problem arises because Axum's WebSocket is not Sync. Therefore, it isn't safe to put it in a Sha...

9. [Server-Sent Events vs WebSockets for AI Streaming - CallSphere](https://callsphere.tech/blog/server-sent-events-vs-websockets-ai-streaming-choosing-right-protocol) - Compare SSE and WebSockets for streaming AI agent outputs, understand the tradeoffs between unidirec...

10. [Streaming for LLM Apps: SSE vs WebSockets | Hivenet](https://www.hivenet.com/post/llm-streaming-sse-websockets) - SSE streams data from the server to the client over an HTTP connection. Simple, proxy‑friendly, grea...

11. [Building Real-Time Apps with Rust WebSockets: Tokio + Axum in ...](https://rustify.rs/articles/rust-websocket-realtime-apps-tokio-axum-2026) - Rust WebSocket apps with Axum and Tokio handle millions of concurrent connections on modest hardware...

32. [axum_limit - Rust - Docs.rs](https://docs.rs/axum-limit) - This crate provides an efficient rate limiting mechanism using token buckets, specifically designed ...

