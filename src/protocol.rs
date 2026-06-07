//! JSON wire protocol between router and provider nodes.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NodeToRouterFrame {
    Auth {
        node_id: String,
        signature: String,
        #[serde(default)]
        ed25519_pubkey: Option<String>,
        #[serde(default)]
        moniker: Option<String>,
    },
    Pong,
    Response {
        rid: Uuid,
        status: u16,
        #[serde(default)]
        headers: Value,
    },
    Chunk {
        rid: Uuid,
        data: String,
    },
    End {
        rid: Uuid,
        status: u16,
    },
    Error {
        rid: Uuid,
        code: u16,
        message: String,
    },
    ActivateResponse {
        rid: Uuid,
        api_key: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RouterToNodeFrame {
    Challenge {
        nonce: String,
        block: u64,
    },
    Ready {
        router_url: String,
    },
    Request {
        rid: Uuid,
        method: String,
        path: String,
        #[serde(default)]
        headers: Value,
        body: Option<String>,
    },
    ActivateRequest {
        rid: Uuid,
        session_id: String,
        signature: String,
        block_number: u64,
        #[serde(default)]
        message: Option<String>,
    },
    Ping,
}

/// Inbound frames routed to a pending consumer request.
#[derive(Debug, Clone)]
pub enum InboundFrame {
    Response { status: u16, headers: Value },
    Chunk(String),
    End { status: u16 },
    Error { code: u16, message: String },
    ActivateResponse { api_key: String },
}

impl NodeToRouterFrame {
    pub fn parse(text: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(text)
    }
}

impl RouterToNodeFrame {
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_challenge() {
        let frame = RouterToNodeFrame::Challenge {
            nonce: "abc".into(),
            block: 123,
        };
        let json = frame.to_json().unwrap();
        assert!(json.contains("challenge"));
    }

    #[test]
    fn parse_auth() {
        let json = r#"{"type":"auth","node_id":"0xabc","signature":"0xsig"}"#;
        let f: NodeToRouterFrame = serde_json::from_str(json).unwrap();
        assert!(matches!(f, NodeToRouterFrame::Auth { .. }));
    }
}
