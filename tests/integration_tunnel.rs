mod mock_node;

use sparkl_router::chain::ChainVerifier;
use sparkl_router::config::Config;
use sparkl_router::consumer::models::ModelsCatalog;
use sparkl_router::metrics;
use sparkl_router::routes::build_router;
use sparkl_router::state::RouterState;
use std::net::SocketAddr;

fn test_config() -> Config {
    let toml = r#"
[server]
bind = "127.0.0.1:0"
router_url = "http://127.0.0.1:3001"

[chain]
rpc_url = "http://127.0.0.1:8545"
registry_contract = "0x0000000000000000000000000000000000000000"
escrow_contract = "0x0000000000000000000000000000000000000000"
session_cache_ttl_secs = 12
enabled = false

[node_auth]
challenge_window_blocks = 10
ping_interval_secs = 30
pong_timeout_secs = 10

[metrics]
bind = "127.0.0.1:0"

[portal]
admin_token = "test-admin-token"
stale_threshold_secs = 40
"#;
    toml::from_str(toml).expect("test config")
}

fn test_metrics_handle() -> metrics_exporter_prometheus::PrometheusHandle {
    use std::sync::OnceLock;
    static HANDLE: OnceLock<metrics_exporter_prometheus::PrometheusHandle> = OnceLock::new();
    HANDLE
        .get_or_init(|| {
            sparkl_router::metrics::init_metrics();
            sparkl_router::metrics::install_prometheus_recorder().expect("metrics")
        })
        .clone()
}

async fn spawn_test_router() -> SocketAddr {
    let config = test_config();
    let handle = test_metrics_handle();
    let chain = ChainVerifier::new(config.chain.clone());
    let models = ModelsCatalog::new();
    let state = RouterState::new(config.clone(), chain, models, handle);
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

#[tokio::test]
async fn handshake_and_status_online() {
    let addr = spawn_test_router().await;
    let keys = mock_node::MockNodeKeys::generate();
    let ws_url = format!("ws://{}/node/connect", addr);

    let mut ws = mock_node::connect_mock_node(&ws_url, &keys).await.unwrap();
    mock_node::respond_pong(&mut ws).await.unwrap();

    let client = reqwest::Client::new();
    let node_hex = format!("0x{}", hex::encode(keys.node_id));
    let resp = client
        .get(format!("http://{}/status/nodes/{}", addr, node_hex))
        .header("Authorization", "Bearer test-admin-token")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "online");
}

#[tokio::test]
async fn health_lists_tunnel() {
    let addr = spawn_test_router().await;
    let keys = mock_node::MockNodeKeys::generate();
    let ws_url = format!("ws://{}/node/connect", addr);
    let _ws = mock_node::connect_mock_node(&ws_url, &keys).await.unwrap();

    let resp = reqwest::get(format!("http://{}/health", addr))
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["tunnels"], 1);
}
