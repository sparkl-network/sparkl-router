use std::convert::Infallible;
use std::time::Duration;

use async_stream::stream;
use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use crate::protocol::InboundFrame;
use crate::tunnel::dispatch::PendingRequest;

pub async fn collect_http_response(
    mut pending: PendingRequest,
    timeout: Duration,
) -> Result<Response, (StatusCode, String)> {
    let result = tokio::time::timeout(timeout, async {
        let mut status = StatusCode::OK;
        let mut headers = HeaderMap::new();
        let mut body = Vec::new();

        while let Some(frame) = pending.rx.recv().await {
            match frame {
                InboundFrame::Response {
                    status: s,
                    headers: h,
                } => {
                    status = StatusCode::from_u16(s).unwrap_or(StatusCode::OK);
                    if let Some(obj) = h.as_object() {
                        for (k, v) in obj {
                            if let Some(sv) = v.as_str() {
                                if let Ok(name) =
                                    header::HeaderName::try_from(k.as_str())
                                {
                                    if let Ok(val) = header::HeaderValue::try_from(sv) {
                                        headers.insert(name, val);
                                    }
                                }
                            }
                        }
                    }
                }
                InboundFrame::Chunk(data) => body.extend_from_slice(data.as_bytes()),
                InboundFrame::End { status: s } => {
                    status = StatusCode::from_u16(s).unwrap_or(status);
                    break;
                }
                InboundFrame::Error { message, code } => {
                    return Err((
                        StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY),
                        message,
                    ));
                }
                _ => {}
            }
        }

        Ok(Response::builder()
            .status(status)
            .body(Body::from(body))
            .unwrap())
    })
    .await;

    match result {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(e),
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "upstream timeout".into())),
    }
}

pub fn sse_from_pending(
    mut pending: PendingRequest,
    timeout: Duration,
) -> Response {
    let stream = stream! {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                yield Ok::<_, Infallible>(bytes::Bytes::from("data: {\"error\":\"timeout\"}\n\n"));
                break;
            }
            match tokio::time::timeout(remaining, pending.rx.recv()).await {
                Ok(Some(InboundFrame::Chunk(data))) => {
                    yield Ok(bytes::Bytes::from(data));
                }
                Ok(Some(InboundFrame::End { .. })) => break,
                Ok(Some(InboundFrame::Error { message, .. })) => {
                    let line = format!("data: {{\"error\":{}}}\n\n", serde_json::json!(message));
                    yield Ok(bytes::Bytes::from(line));
                    break;
                }
                Ok(None) | Err(_) => break,
                _ => {}
            }
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap()
}
