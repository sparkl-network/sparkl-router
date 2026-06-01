use std::collections::HashMap;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};

use crate::consumer::middleware;
use crate::consumer::models::list_models_handler;
use crate::consumer::sse::{collect_http_response, sse_from_pending};
use crate::state::{AuthenticatedSession, RouterState};
use crate::tunnel::dispatch::forward_http_request;

const UPSTREAM_TIMEOUT: Duration = Duration::from_secs(120);

pub async fn chat_completions(
    State(state): State<RouterState>,
    Extension(auth): Extension<AuthenticatedSession>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body_str = String::from_utf8_lossy(&body).to_string();
    let stream = parse_stream_flag(&body_str);

    if let Some(model) = parse_model(&body_str) {
        state.models.refresh_from_tunnels(&state).await;
        if !state.models.contains(&model).await {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("unknown model: {model}"),
            );
        }
    }

    let node_id: [u8; 32] = auth.node_id.into();
    let tunnel = match state.tunnels.get(&node_id) {
        Some(t) => t,
        None => {
            return error_response(StatusCode::SERVICE_UNAVAILABLE, "provider offline");
        }
    };

    let forward_headers = headers_to_json(&headers);

    let pending = match forward_http_request(
        &tunnel,
        "POST",
        "/v1/chat/completions",
        forward_headers,
        Some(body_str),
        UPSTREAM_TIMEOUT,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            return error_response(StatusCode::BAD_GATEWAY, &e.to_string());
        }
    };

    crate::metrics::inc_requests_forwarded();

    if stream {
        return sse_from_pending(pending, UPSTREAM_TIMEOUT);
    }

    match collect_http_response(pending, UPSTREAM_TIMEOUT).await {
        Ok(resp) => resp,
        Err((status, msg)) => error_response(status, &msg),
    }
}

pub async fn list_models(State(state): State<RouterState>) -> Json<Value> {
    Json(list_models_handler(state).await)
}

fn parse_stream_flag(body: &str) -> bool {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
        .unwrap_or(false)
}

fn parse_model(body: &str) -> Option<String> {
    serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|v| v.get("model").and_then(|m| m.as_str().map(String::from)))
}

fn headers_to_json(headers: &HeaderMap) -> Value {
    let mut map = HashMap::new();
    for (name, value) in headers.iter() {
        if let Ok(s) = value.to_str() {
            map.insert(name.as_str().to_string(), s.to_string());
        }
    }
    serde_json::to_value(map).unwrap_or(json!({}))
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message,
                "type": "router_error"
            }
        })),
    )
        .into_response()
}

pub fn v1_protected_routes(state: RouterState) -> axum::Router<RouterState> {
    use axum::routing::post;

    axum::Router::new()
        .route("/chat/completions", post(chat_completions))
        .layer(axum::middleware::from_fn_with_state(
            state,
            middleware::require_v1_session,
        ))
}
