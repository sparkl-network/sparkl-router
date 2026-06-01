use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::chain::session_is_open;
use crate::consumer::bearer::parse_authorization_header;
use crate::state::{AuthenticatedSession, RouterState};

pub async fn require_v1_session(
    State(state): State<RouterState>,
    mut request: Request,
    next: Next,
) -> Response {
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let authz = match auth_header {
        Some(h) => h,
        None => return auth_json(StatusCode::UNAUTHORIZED, "missing Authorization header"),
    };

    let bearer = match parse_authorization_header(&authz) {
        Ok(b) => b,
        Err(e) => return auth_json(StatusCode::BAD_REQUEST, &e.to_string()),
    };

    let chain_sess = match state.chain.get_session(bearer.session_id).await {
        Ok(s) => s,
        Err(e) => {
            return auth_json(
                StatusCode::UNAUTHORIZED,
                &format!("failed to verify session: {e}"),
            );
        }
    };

    if !session_is_open(&chain_sess) {
        return auth_json(StatusCode::UNAUTHORIZED, "session not open");
    }

    let node_id_bytes: [u8; 32] = chain_sess.node_id.into();

    if state.tunnels.get(&node_id_bytes).is_none() {
        return auth_json(
            StatusCode::SERVICE_UNAVAILABLE,
            "provider node offline",
        );
    }

    request.extensions_mut().insert(AuthenticatedSession {
        session_id: bearer.session_id,
        user: chain_sess.user,
        node_id: chain_sess.node_id,
    });

    next.run(request).await
}

fn auth_json(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": { "message": message, "type": "auth_error" } }))).into_response()
}
