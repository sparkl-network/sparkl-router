//! Parse OpenAI-compatible `usage` and sparkl-solo receipt metering from inference responses.

use base64::Engine;
use serde::Deserialize;
use serde_json::Value;
use tracing::debug;

#[derive(Debug, Clone, Copy, Default)]
pub struct ParsedUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub fn extract_usage_from_json(value: &Value) -> Option<ParsedUsage> {
    let usage = value.get("usage")?;
    let input = usage
        .get("prompt_tokens")
        .or_else(|| usage.get("input_tokens"))
        .and_then(|v| v.as_u64())?;
    let output = usage
        .get("completion_tokens")
        .or_else(|| usage.get("output_tokens"))
        .and_then(|v| v.as_u64())?;
    Some(ParsedUsage {
        input_tokens: input,
        output_tokens: output,
    })
}

pub fn extract_usage_from_body(body: &str) -> Option<ParsedUsage> {
    let value: Value = serde_json::from_str(body).ok()?;
    extract_usage_from_json(&value)
}

#[derive(Debug, Deserialize)]
struct SparklReceiptPayload {
    token_count: u64,
}

fn sparkl_receipt_output_tokens(value: &Value) -> Option<u64> {
    let receipt_b64 = value.pointer("/sparkl/receipt")?.as_str()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(receipt_b64)
        .ok()?;
    let receipt: SparklReceiptPayload = serde_json::from_slice(&bytes).ok()?;
    Some(receipt.token_count)
}

fn iter_sse_event_values(body: &str) -> impl Iterator<Item = Value> + '_ {
    body.split("data:").skip(1).filter_map(|segment| {
        let payload = segment.lines().next()?.trim();
        if payload.is_empty() || payload == "[DONE]" {
            return None;
        }
        serde_json::from_str::<Value>(payload).ok()
    })
}

/// Scan SSE events for OpenAI `usage` and/or sparkl `receipt` checkpoints.
pub fn extract_usage_from_sse_body(body: &str) -> Option<ParsedUsage> {
    let mut openai = None;
    let mut max_receipt_output = 0u64;

    for v in iter_sse_event_values(body) {
        if let Some(u) = extract_usage_from_json(&v) {
            openai = Some(u);
        }
        if let Some(tc) = sparkl_receipt_output_tokens(&v) {
            max_receipt_output = max_receipt_output.max(tc);
        }
    }

    match (openai, max_receipt_output) {
        (Some(mut u), receipt_out) if receipt_out > u.output_tokens => {
            debug!(
                openai_output = u.output_tokens,
                receipt_output = receipt_out,
                "metering: using sparkl receipt output token count"
            );
            u.output_tokens = receipt_out;
            Some(u)
        }
        (Some(u), _) => Some(u),
        (None, 0) => None,
        (None, receipt_out) => Some(ParsedUsage {
            input_tokens: 0,
            output_tokens: receipt_out,
        }),
    }
}

/// Ensure streaming requests ask backends for a final usage chunk when possible.
pub fn inject_stream_usage_option(body: &str) -> String {
    let Ok(mut v) = serde_json::from_str::<Value>(body) else {
        return body.to_string();
    };
    if !v.get("stream").and_then(|s| s.as_bool()).unwrap_or(false) {
        return body.to_string();
    }
    let obj = v.as_object_mut();
    if let Some(map) = obj {
        map.entry("stream_options")
            .or_insert_with(|| serde_json::json!({ "include_usage": true }));
    }
    serde_json::to_string(&v).unwrap_or_else(|_| body.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_usage_from_completion_json() {
        let body = r#"{"usage":{"prompt_tokens":100,"completion_tokens":50}}"#;
        let u = extract_usage_from_body(body).unwrap();
        assert_eq!(u.input_tokens, 100);
        assert_eq!(u.output_tokens, 50);
    }

    #[test]
    fn extracts_usage_from_sse_tail() {
        let sse = "data: {\"choices\":[]}\n\ndata: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5}}\n\ndata: [DONE]\n\n";
        let u = extract_usage_from_sse_body(sse).unwrap();
        assert_eq!(u.input_tokens, 10);
        assert_eq!(u.output_tokens, 5);
    }

    #[test]
    fn extracts_output_from_sparkl_receipt_sse() {
        let receipt = r#"{"token_count":900}"#;
        let b64 = base64::engine::general_purpose::STANDARD.encode(receipt);
        let sse = format!(
            "data: {{\"choices\":[],\"sparkl\":{{\"seq\":18,\"receipt\":\"{b64}\"}}}}\n\n"
        );
        let u = extract_usage_from_sse_body(&sse).unwrap();
        assert_eq!(u.input_tokens, 0);
        assert_eq!(u.output_tokens, 900);
    }
}
