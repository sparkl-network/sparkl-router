use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::state::RouterState;

pub async fn require_admin_token(
    State(state): State<RouterState>,
    request: Request,
    next: Next,
) -> Response {
    let expected = state.config.portal.admin_token.trim();
    if expected.is_empty() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "admin token not configured",
        )
            .into_response();
    }

    let auth = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let valid = auth
        .and_then(|h| {
            h.strip_prefix("Bearer ")
                .or_else(|| h.strip_prefix("bearer "))
        })
        .map(|t| t.trim() == expected)
        .unwrap_or(false);

    if !valid {
        return (StatusCode::UNAUTHORIZED, "invalid admin token").into_response();
    }

    next.run(request).await
}
