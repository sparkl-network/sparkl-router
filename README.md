# sparkl-router

High-performance Rust gateway for Sparkl: public OpenAI-compatible HTTPS/SSE for consumers, and outbound WSS tunnels for provider nodes.

## Surfaces

| Surface | Endpoints | Auth |
|---------|-----------|------|
| Consumer (OpenAI-compatible) | `GET /v1/models`, `POST /v1/chat/completions` | `Authorization: Bearer sk_…` after activation |
| Catalog (public discovery) | `GET /v1/catalog/features`, `GET /v1/catalog/providers` | No auth |
| Sparkl extension | `POST /sessions/{sessionId}/activate` | Wallet signature (EIP-191) |
| Provider tunnel | `GET /node/connect` (WebSocket upgrade) | Ed25519 challenge + on-chain registry |
| Ops | `GET /health` | None |
| Portal / admin | `GET /status/nodes`, `GET /status/nodes/{nodeId}`, `GET /status/subscribe` (WebSocket), `GET /metrics` | `Authorization: Bearer <admin_token>` or short-lived `?token=&exp=` HMAC |

Admin node status includes optional **`moniker`** (from the WSS `auth` frame, max 128 chars) alongside tunnel health and model counts.

`GET /v1/models` returns a **cached union** of model objects (ids plus optional `context_length` / `sparkl` metadata from solo nodes). The cache is refreshed when a node completes WSS subscription and periodically on pong heartbeats (`[portal].models_refresh_on_pong_secs`). Duplicate model IDs across nodes are merged with a `sparkl.providers` breakdown and aggregated load fields.

Per-model **admission control** enforces operator-declared `concurrency` on `POST /v1/chat/completions`: requests wait in a bounded queue (`[capacity].queue_depth_ratio` × concurrency), then receive **HTTP 429** `capacity_exhausted` when full. **HTTP 503** `provider_unavailable` is returned when the node tunnel is missing. Live `active_requests` / `queued_requests` counters are pushed on `GET /status/subscribe` WebSocket events (`model_capacity`, `node_status`).

`POST /sessions/{id}/activate` is **not** part of the OpenAI API. Clients must activate after opening an on-chain escrow session, then use the returned `apiKey` with standard OpenAI SDKs (`baseURL` = router URL).

## Quick start

```bash
cp config.example.toml config.toml
# Edit chain contract addresses and portal.admin_token

cargo run -- config.toml

# More detail (flush skips, eth_call traces from dependencies):
RUST_LOG=sparkl_router=debug cargo run -- config.toml
```

### `recordUsageRole` (on-chain metering)

The router signs `SettlementEscrow.recordUsage` as `recordUsageRole`. On first start it writes `data/record-usage-key.json`. The registry owner must register that address on-chain with `setRecordUsage`:

```bash
# After contracts support recordUsageRole (redeploy if eth_call reverts)
./scripts/set-record-usage-role.sh config.toml
```

Requires `cast`, `jq`, and `settlement.registry_owner_private_key` in config (Anvil account 0 on local dev). If `recordUsageRole()` reverts, redeploy from sparkl-solo: `./scripts/deploy-local-sync-env.sh`, then run the script again.

- Main API: `http://127.0.0.1:3001` (see `[server].bind`)
- Prometheus: `http://127.0.0.1:9091/metrics` (admin bearer required)

## Configuration

See [config.example.toml](config.example.toml). Environment overrides use prefix `SPARKL_ROUTER__`, e.g. `SPARKL_ROUTER__PORTAL__ADMIN_TOKEN`.

| Section | Purpose |
|---------|---------|
| `[server]` | HTTP bind address, public `router_url`, and `upstream_timeout_secs` (default 120) for `/v1/chat/completions` |
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
- [sparkl-portal](../sparkl-portal) — hub UI (router HTTP/WS proxy, telemetry subscribe, capacity display)
