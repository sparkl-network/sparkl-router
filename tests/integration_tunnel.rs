mod mock_node;

use sparkl_router::chain::ChainVerifier;
use sparkl_router::config::Config;
use sparkl_router::consumer::models::ModelsCatalog;
use sparkl_router::consumer::sse::collect_http_response;
use sparkl_router::routes::build_router;
use sparkl_router::tunnel::dispatch::forward_http_request;
use sparkl_router::state::RouterState;
use std::net::SocketAddr;
use std::time::Duration;
use axum::body::to_bytes;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

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
    let state = RouterState::new(config.clone(), chain, models, None, handle);
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

async fn spawn_test_router_with_state() -> (SocketAddr, RouterState) {
    let config = test_config();
    let handle = test_metrics_handle();
    let chain = ChainVerifier::new(config.chain.clone());
    let models = ModelsCatalog::new();
    let state = RouterState::new(config.clone(), chain, models, None, handle);
    let app = build_router(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    (addr, state)
}

async fn wait_for_chat_request(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> Uuid {
    loop {
        let msg = ws.next().await.unwrap().unwrap();
        let Message::Text(text) = msg else {
            continue;
        };
        let frame: sparkl_router::protocol::RouterToNodeFrame =
            serde_json::from_str(&text).unwrap();
        match frame {
            sparkl_router::protocol::RouterToNodeFrame::Ping => {
                ws.send(Message::Text(
                    serde_json::json!({"type":"pong"}).to_string().into(),
                ))
                .await
                .unwrap();
            }
            sparkl_router::protocol::RouterToNodeFrame::Request { rid, path, .. } => {
                if path == "/v1/models" {
                    let response = sparkl_router::protocol::NodeToRouterFrame::Response {
                        rid,
                        status: 200,
                        headers: json!({"content-type":"application/json"}),
                    };
                    ws.send(Message::Text(serde_json::to_string(&response).unwrap().into()))
                        .await
                        .unwrap();
                    let chunk = sparkl_router::protocol::NodeToRouterFrame::Chunk {
                        rid,
                        data: r#"{"data":[{"id":"mock-model"}]}"#.to_string(),
                    };
                    ws.send(Message::Text(serde_json::to_string(&chunk).unwrap().into()))
                        .await
                        .unwrap();
                    let end = sparkl_router::protocol::NodeToRouterFrame::End { rid, status: 200 };
                    ws.send(Message::Text(serde_json::to_string(&end).unwrap().into()))
                        .await
                        .unwrap();
                    continue;
                }
                if path == "/v1/chat/completions" {
                    return rid;
                }
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn handshake_status_includes_moniker() {
    let addr = spawn_test_router().await;
    let keys = mock_node::MockNodeKeys::generate();
    let ws_url = format!("ws://{}/node/connect", addr);

    let mut ws =
        mock_node::connect_mock_node_with_moniker(&ws_url, &keys, Some("test-moniker")).await
            .unwrap();
    mock_node::respond_pong(&mut ws).await.unwrap();

    let client = reqwest::Client::new();
    let node_hex = format!("0x{}", hex::encode(keys.node_id));
    let resp = client
        .get(format!("http://{}/status/nodes/{}", addr, node_hex))
        .header("Authorization", "Bearer test-admin-token")
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["moniker"], "test-moniker");
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

#[tokio::test]
async fn tunnel_non_stream_returns_json_body() {
    let (addr, state) = spawn_test_router_with_state().await;
    let keys = mock_node::MockNodeKeys::generate();
    let ws_url = format!("ws://{}/node/connect", addr);
    let mut ws = mock_node::connect_mock_node(&ws_url, &keys).await.unwrap();

    let node_id = keys.node_id;
    let tunnel = loop {
        if let Some(tunnel) = state.tunnels.get(&node_id) {
            break tunnel;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let pending = forward_http_request(
        &tunnel,
        "POST",
        "/v1/chat/completions",
        json!({}),
        Some(r#"{"stream":false}"#.to_string()),
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    let rid = wait_for_chat_request(&mut ws).await;
    let response = sparkl_router::protocol::NodeToRouterFrame::Response {
        rid,
        status: 200,
        headers: json!({"content-type":"application/json"}),
    };
    ws.send(Message::Text(serde_json::to_string(&response).unwrap().into()))
        .await
        .unwrap();
    let chunk = sparkl_router::protocol::NodeToRouterFrame::Chunk {
        rid,
        data: r#"{"id":"resp-1","object":"chat.completion"}"#.to_string(),
    };
    ws.send(Message::Text(serde_json::to_string(&chunk).unwrap().into()))
        .await
        .unwrap();
    let end = sparkl_router::protocol::NodeToRouterFrame::End { rid, status: 200 };
    ws.send(Message::Text(serde_json::to_string(&end).unwrap().into()))
        .await
        .unwrap();

    let collected = collect_http_response(pending, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(collected.response.status(), reqwest::StatusCode::OK);
    let body = to_bytes(collected.response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        json!({"id":"resp-1","object":"chat.completion"})
    );

}

#[tokio::test]
async fn tunnel_non_stream_stitches_multi_chunk_json() {
    let (addr, state) = spawn_test_router_with_state().await;
    let keys = mock_node::MockNodeKeys::generate();
    let ws_url = format!("ws://{}/node/connect", addr);
    let mut ws = mock_node::connect_mock_node(&ws_url, &keys).await.unwrap();

    let node_id = keys.node_id;
    let tunnel = loop {
        if let Some(tunnel) = state.tunnels.get(&node_id) {
            break tunnel;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let pending = forward_http_request(
        &tunnel,
        "POST",
        "/v1/chat/completions",
        json!({}),
        Some(r#"{"stream":false}"#.to_string()),
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    let rid = wait_for_chat_request(&mut ws).await;
    let response = sparkl_router::protocol::NodeToRouterFrame::Response {
        rid,
        status: 200,
        headers: json!({"content-type":"application/json"}),
    };
    ws.send(Message::Text(serde_json::to_string(&response).unwrap().into()))
        .await
        .unwrap();
    let chunk_a = sparkl_router::protocol::NodeToRouterFrame::Chunk {
        rid,
        data: r#"{"id":"resp-2","#.to_string(),
    };
    ws.send(Message::Text(serde_json::to_string(&chunk_a).unwrap().into()))
        .await
        .unwrap();
    let chunk_b = sparkl_router::protocol::NodeToRouterFrame::Chunk {
        rid,
        data: r#""ok":true}"#.to_string(),
    };
    ws.send(Message::Text(serde_json::to_string(&chunk_b).unwrap().into()))
        .await
        .unwrap();
    let end = sparkl_router::protocol::NodeToRouterFrame::End { rid, status: 200 };
    ws.send(Message::Text(serde_json::to_string(&end).unwrap().into()))
        .await
        .unwrap();

    let collected = collect_http_response(pending, Duration::from_secs(5))
        .await
        .unwrap();
    let body = to_bytes(collected.response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        json!({"id":"resp-2","ok":true})
    );

}

#[tokio::test]
async fn tunnel_streaming_frames_still_flow() {
    let (addr, state) = spawn_test_router_with_state().await;
    let keys = mock_node::MockNodeKeys::generate();
    let ws_url = format!("ws://{}/node/connect", addr);
    let mut ws = mock_node::connect_mock_node(&ws_url, &keys).await.unwrap();

    let node_id = keys.node_id;
    let tunnel = loop {
        if let Some(tunnel) = state.tunnels.get(&node_id) {
            break tunnel;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };

    let pending = forward_http_request(
        &tunnel,
        "POST",
        "/v1/chat/completions",
        json!({}),
        Some(r#"{"stream":true}"#.to_string()),
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    let rid = wait_for_chat_request(&mut ws).await;
    let chunk = sparkl_router::protocol::NodeToRouterFrame::Chunk {
        rid,
        data: "data: {\"choices\":[]}\n\n".to_string(),
    };
    ws.send(Message::Text(serde_json::to_string(&chunk).unwrap().into()))
        .await
        .unwrap();
    let end = sparkl_router::protocol::NodeToRouterFrame::End { rid, status: 200 };
    ws.send(Message::Text(serde_json::to_string(&end).unwrap().into()))
        .await
        .unwrap();

    let mut rx = pending.rx;
    let mut saw_chunk = false;
    let mut saw_end = false;
    while let Some(frame) = rx.recv().await {
        match frame {
            sparkl_router::protocol::InboundFrame::Chunk(data) => {
                assert!(!data.is_empty());
                saw_chunk = true;
            }
            sparkl_router::protocol::InboundFrame::End { status } => {
                assert_eq!(status, 200);
                saw_end = true;
                break;
            }
            _ => {}
        }
    }

    assert!(saw_chunk);
    assert!(saw_end);
}
