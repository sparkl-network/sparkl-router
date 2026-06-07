use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_stream::stream;
use axum::body::Body;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::Response;
use crate::consumer::metering::{extract_usage_from_sse_body, ParsedUsage};
use crate::protocol::InboundFrame;
use crate::capacity::CapacityGuard;
use crate::tunnel::dispatch::PendingRequest;

pub struct CollectedResponse {
    pub response: Response,
    pub body: String,
}

pub async fn collect_http_response(
    mut pending: PendingRequest,
    timeout: Duration,
) -> Result<CollectedResponse, (StatusCode, String)> {
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

        let body_str = String::from_utf8_lossy(&body).into_owned();
        let response = Response::builder()
            .status(status)
            .body(Body::from(body.clone()))
            .unwrap();
        Ok(CollectedResponse {
            response,
            body: body_str,
        })
    })
    .await;

    match result {
        Ok(Ok(resp)) => Ok(resp),
        Ok(Err(e)) => Err(e),
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "upstream timeout".into())),
    }
}

struct SseStreamCompletion<F>
where
    F: FnOnce(Option<ParsedUsage>, usize, bool) + Send + 'static,
{
    guard: Option<CapacityGuard>,
    on_complete: Option<F>,
    fired: bool,
}

impl<F> SseStreamCompletion<F>
where
    F: FnOnce(Option<ParsedUsage>, usize, bool) + Send + 'static,
{
    fn fire(&mut self, usage: Option<ParsedUsage>, bytes: usize, has_sparkl: bool) {
        if self.fired {
            return;
        }
        self.fired = true;
        // Release capacity before telemetry/metering callbacks read active counts.
        self.guard.take();
        if let Some(f) = self.on_complete.take() {
            f(usage, bytes, has_sparkl);
        }
    }
}

impl<F> Drop for SseStreamCompletion<F>
where
    F: FnOnce(Option<ParsedUsage>, usize, bool) + Send + 'static,
{
    fn drop(&mut self) {
        if self.fired {
            return;
        }
        self.guard.take();
        if let Some(f) = self.on_complete.take() {
            f(None, 0, false);
        }
    }
}

pub fn sse_from_pending_with_guard<F>(
    pending: PendingRequest,
    timeout: Duration,
    guard: CapacityGuard,
    on_complete: F,
) -> Response
where
    F: FnOnce(Option<ParsedUsage>, usize, bool) + Send + 'static,
{
    // Bridge tunnel frames in a dedicated task so `PendingRequest` stays registered
    // until upstream closes — matching the inline await lifetime in `collect_http_response`.
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<InboundFrame>(256);
    tokio::spawn(async move {
        let mut pending = pending;
        while let Some(frame) = pending.rx.recv().await {
            if frame_tx.send(frame).await.is_err() {
                break;
            }
        }
    });

    // Fire metering when the upstream tunnel ends, not when the HTTP client drains
    // the response body. Drop on stream cancel still releases capacity + emits telemetry.
    let completion = Arc::new(Mutex::new(SseStreamCompletion {
        guard: Some(guard),
        on_complete: Some(on_complete),
        fired: false,
    }));
    let stream = stream! {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut accumulated = String::new();
        let mut latest_usage: Option<ParsedUsage> = None;
        let fire_complete = |usage: Option<ParsedUsage>, bytes: usize, has_sparkl: bool| {
            if let Ok(mut state) = completion.lock() {
                state.fire(usage, bytes, has_sparkl);
            }
        };
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                yield Ok::<_, Infallible>(bytes::Bytes::from("data: {\"error\":\"timeout\"}\n\n"));
                break;
            }
            match tokio::time::timeout(remaining, frame_rx.recv()).await {
                Ok(Some(InboundFrame::Chunk(data))) => {
                    accumulated.push_str(&data);
                    if let Some(u) = extract_usage_from_sse_body(&accumulated) {
                        latest_usage = Some(u);
                    }
                    yield Ok(bytes::Bytes::from(data));
                }
                Ok(Some(InboundFrame::End { .. })) => {
                    let usage = latest_usage.or_else(|| extract_usage_from_sse_body(&accumulated));
                    let has_sparkl = accumulated.contains("\"sparkl\"");
                    fire_complete(usage, accumulated.len(), has_sparkl);
                    break;
                }
                Ok(Some(InboundFrame::Error { message, .. })) => {
                    let line = format!("data: {{\"error\":{}}}\n\n", serde_json::json!(message));
                    yield Ok(bytes::Bytes::from(line));
                    let usage = latest_usage.or_else(|| extract_usage_from_sse_body(&accumulated));
                    fire_complete(usage, accumulated.len(), accumulated.contains("\"sparkl\""));
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    yield Ok(bytes::Bytes::from("data: {\"error\":\"timeout\"}\n\n"));
                    break;
                }
                _ => {}
            }
        }
        let usage = latest_usage.or_else(|| extract_usage_from_sse_body(&accumulated));
        fire_complete(usage, accumulated.len(), accumulated.contains("\"sparkl\""));
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap()
}
