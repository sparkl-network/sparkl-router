use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use tower::ServiceBuilder;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::trace::TraceLayer;

use crate::admin::require_admin_token;
use crate::consumer::{activate, catalog, forward};
use crate::state::RouterState;
use crate::status::{nodes, subscribe};
use crate::tunnel::connect::node_connect;

pub fn build_router(state: RouterState) -> Router {
    let v1_public = Router::new()
        .route("/models", get(forward::list_models))
        .route("/catalog/providers", get(catalog::list_providers))
        .route("/catalog/features", get(catalog::list_feature_catalog));

    let v1_protected = forward::v1_protected_routes(state.clone());

    let consumer = Router::new()
        .nest("/v1", v1_public.merge(v1_protected))
        .route(
            "/sessions/{session_id}/activate",
            post(activate::activate_session),
        );

    let admin = Router::new()
        .route("/status/nodes", get(nodes::list_nodes))
        .route("/status/nodes/{node_id}", get(nodes::get_node))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            require_admin_token,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/node/connect", get(node_connect))
        .route("/status/subscribe", get(subscribe::status_subscribe))
        .merge(consumer)
        .merge(admin)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(ConcurrencyLimitLayer::new(10_000)),
        )
        .with_state(state)
}

async fn health(State(state): State<RouterState>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "tunnels": state.tunnel_count(),
        "uptime_secs": state.uptime_secs()
    }))
}

pub async fn metrics_handler(State(state): State<RouterState>) -> String {
    state.metrics_handle.render()
}
