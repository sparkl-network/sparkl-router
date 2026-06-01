# sparkl-router

High-performance Rust gateway for Sparkl: public OpenAI-compatible HTTPS/SSE for consumers, and outbound WSS tunnels for provider nodes.

## Surfaces

| Surface | Endpoints | Auth |
|---------|-----------|------|
| Consumer (OpenAI-compatible) | `GET /v1/models`, `POST /v1/chat/completions` | `Authorization: Bearer sk_…` after activation |
| Sparkl extension | `POST /sessions/{sessionId}/activate` | Wallet signature (EIP-191) |
| Provider tunnel | `GET /node/connect` (WebSocket upgrade) | Ed25519 challenge + on-chain registry |
| Ops | `GET /health` | None |
| Portal / admin | `GET /status/nodes`, `GET /status/nodes/{nodeId}`, `GET /metrics` | `Authorization: Bearer <admin_token>` |

`GET /v1/models` returns a **cached union** of model ids from all connected nodes. The cache is refreshed when a node completes WSS registration and periodically on pong heartbeats (`[portal].models_refresh_on_pong_secs`).

`POST /sessions/{id}/activate` is **not** part of the OpenAI API. Clients must activate after opening an on-chain escrow session, then use the returned `apiKey` with standard OpenAI SDKs (`baseURL` = router URL).

## Quick start

```bash
cp config.example.toml config.toml
# Edit chain contract addresses and portal.admin_token

cargo run -- config.toml
```

- Main API: `http://127.0.0.1:3001` (see `[server].bind`)
- Prometheus: `http://127.0.0.1:9091/metrics` (admin bearer required)

## Configuration

See [config.example.toml](config.example.toml). Environment overrides use prefix `SPARKL_ROUTER__`, e.g. `SPARKL_ROUTER__PORTAL__ADMIN_TOKEN`.

| Section | Purpose |
|---------|---------|
| `[server]` | HTTP bind address and public `router_url` sent to nodes in `ready` frame |
| `[chain]` | RPC URL, escrow/registry contracts, session cache TTL; set `enabled = false` for local mock-node tests |
| `[node_auth]` | WSS challenge window, ping/pong intervals |
| `[portal]` | Admin token (shared with metrics) and tunnel stale threshold for `online` status |

## Provider WSS protocol

1. Router sends `{ "type": "challenge", "nonce", "block" }`
2. Node sends `{ "type": "auth", "node_id", "signature", "ed25519_pubkey" }`  
   Signature over `keccak256("sparkl-router-connect:" || nonce || block_number)`
3. Router verifies signature and `ProviderRegistry.nodeOperator(node_id) != 0`
4. Router sends `{ "type": "ready", "router_url" }`
5. Multiplexed `request` / `chunk` / `end` frames with `rid` UUID

## Tests

```bash
cargo test
cargo clippy -- -D warnings
```

Integration tests spin up a router with `chain.enabled = false` and a mock WSS node client.

## Deployment

TLS termination (Caddy) in front of plain HTTP is expected for production; see [docs/spec.md](docs/spec.md).

## Related

- [sparkl-solo](../sparkl-solo) — provider node (outbound WSS tunnel client + `sk_` activate)
- [sparkl-portal](../sparkl-portal) — hub UI (router status API integration: follow-up)
