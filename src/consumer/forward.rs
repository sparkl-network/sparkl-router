use std::collections::HashMap;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::capacity::{max_queue_depth, AcquireError, ModelCapacityKey};
use crate::consumer::metering::{
    extract_usage_from_body, inject_stream_usage_option,
};
use crate::consumer::middleware;
use crate::consumer::models::list_models_handler;
use crate::consumer::sse::{collect_http_response, sse_from_pending_with_guard};
use crate::consumer::usage_batch::UsageBatcher;
use crate::state::{AuthenticatedSession, RouterState};
use crate::tunnel::dispatch::forward_http_request;

fn upstream_timeout(state: &RouterState) -> Duration {
    Duration::from_secs(state.config.server.upstream_timeout_secs.max(1))
}

pub async fn chat_completions(
    State(state): State<RouterState>,
    Extension(auth): Extension<AuthenticatedSession>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut body_str = String::from_utf8_lossy(&body).to_string();
    let stream = parse_stream_flag(&body_str);

    if stream {
        body_str = inject_stream_usage_option(&body_str);
    }

    let model = match parse_model(&body_str) {
        Some(m) => m,
        None => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "model_not_found",
                "missing model in request body",
            );
        }
    };

    if !state.models.contains(&model).await {
        return error_response(
            StatusCode::BAD_REQUEST,
            "model_not_found",
            &format!("unknown model: {model}"),
        );
    }

    let node_id: [u8; 32] = auth.node_id.into();
    let tunnel = match state.tunnels.get(&node_id) {
        Some(t) => t,
        None => {
            return provider_unavailable_response("provider offline");
        }
    };

    let capacity_key = ModelCapacityKey {
        node_id,
        model_id: model.clone(),
    };
    let concurrency = state.models.concurrency_for(node_id, &model).await;
    let max_queue = max_queue_depth(concurrency, state.config.capacity.queue_depth_ratio);
    let wait_timeout =
        Duration::from_secs(state.config.capacity.queue_wait_timeout_secs.max(1));

    let guard = match state
        .capacity
        .acquire(capacity_key.clone(), concurrency, max_queue, wait_timeout)
        .await
    {
        Ok(g) => g,
        Err(e) => {
            crate::metrics::inc_capacity_rejected();
            return capacity_exhausted_response(e, concurrency);
        }
    };

    emit_model_capacity(&state, &capacity_key, concurrency);

    let forward_headers = headers_to_json(&headers);
    let timeout = upstream_timeout(&state);

    let pending = match forward_http_request(
        &tunnel,
        "POST",
        "/v1/chat/completions",
        forward_headers,
        Some(body_str),
        timeout,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => {
            drop(guard);
            emit_model_capacity(&state, &capacity_key, concurrency);
            return error_response(StatusCode::BAD_GATEWAY, "router_error", &e.to_string());
        }
    };

    crate::metrics::inc_requests_forwarded();

    let session_id = auth.session_id;
    let batcher = state.usage_batcher.clone();
    let state_for_release = state.clone();
    let key_for_release = capacity_key.clone();
    let concurrency_for_release = concurrency;

    if stream {
        return sse_from_pending_with_guard(pending, timeout, guard, move |usage, body_bytes, has_sparkl| {
            emit_model_capacity(&state_for_release, &key_for_release, concurrency_for_release);
            if usage.is_none() {
                info!(
                    session_id,
                    body_bytes,
                    has_sparkl,
                    "recordUsage: no usage in upstream response (expected final usage chunk or sparkl receipt)"
                );
            }
            record_parsed_usage(batcher, session_id, usage);
        });
    }

    let result = collect_http_response(pending, timeout).await;
    drop(guard);
    emit_model_capacity(&state, &capacity_key, concurrency);

    match result {
        Ok(collected) => {
            record_parsed_usage(
                batcher,
                session_id,
                extract_usage_from_body(&collected.body),
            );
            collected.response
        }
        Err((status, msg)) => error_response(status, "router_error", &msg),
    }
}

fn emit_model_capacity(state: &RouterState, key: &ModelCapacityKey, concurrency: u32) {
    let snap = state.capacity.snapshot_with_concurrency(key, concurrency);
    state.telemetry.emit_model_capacity(
        key,
        snap.active_requests,
        snap.queued_requests,
        concurrency,
    );
}

fn record_parsed_usage(
    batcher: Option<UsageBatcher>,
    session_id: u64,
    usage: Option<crate::consumer::metering::ParsedUsage>,
) {
    let Some(usage) = usage else {
        return;
    };
    let Some(batcher) = batcher else {
        warn!(
            session_id,
            input_tokens = usage.input_tokens,
            output_tokens = usage.output_tokens,
            "recordUsage: usage batcher disabled; tokens not recorded on-chain"
        );
        return;
    };
    info!(
        session_id,
        input_tokens = usage.input_tokens,
        output_tokens = usage.output_tokens,
        "recordUsage: queueing parsed usage"
    );
    tokio::spawn(async move {
        batcher
            .add_usage(session_id, usage.input_tokens, usage.output_tokens)
            .await;
    });
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

fn provider_unavailable_response(message: &str) -> Response {
    error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "provider_unavailable",
        message,
    )
}

fn capacity_exhausted_response(err: AcquireError, concurrency: u32) -> Response {
    let (active, queued, retry_after) = match err {
        AcquireError::QueueFull {
            active,
            queued, ..
        } => (active, queued, 15),
        AcquireError::WaitTimeout {
            active,
            queued, ..
        } => (active, queued, 30),
    };
    let body = json!({
        "error": "model at capacity",
        "type": "capacity_exhausted",
        "retry_after": retry_after,
        "active_requests": active,
        "concurrency": concurrency,
        "queued_requests": queued,
    });
    let mut response = (StatusCode::TOO_MANY_REQUESTS, Json(body)).into_response();
    if let Ok(val) = HeaderValue::from_str(&retry_after.to_string()) {
        response
            .headers_mut()
            .insert("retry-after", val);
    }
    response
}

fn error_response(status: StatusCode, error_type: &str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "error": message,
            "type": error_type,
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
