use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::protocol::{InboundFrame, RouterToNodeFrame};
use crate::state::NodeTunnel;

pub struct PendingRequest {
    pub rx: tokio::sync::mpsc::Receiver<InboundFrame>,
}

impl NodeTunnel {
    pub fn register_pending(&self, rid: Uuid) -> PendingRequest {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        self.pending.insert(rid, tx);
        PendingRequest { rx }
    }

    pub fn route_inbound(&self, rid: Uuid, frame: InboundFrame) {
        if let Some((_, tx)) = self.pending.remove(&rid) {
            let _ = tx.try_send(frame);
        }
    }

    pub fn fail_all_pending(&self) {
        self.pending.retain(|_, tx| {
            let _ = tx.try_send(InboundFrame::Error {
                code: 502,
                message: "node disconnected".into(),
            });
            false
        });
    }
}

pub async fn forward_http_request(
    tunnel: &NodeTunnel,
    method: &str,
    path: &str,
    headers: Value,
    body: Option<String>,
    timeout: Duration,
) -> Result<PendingRequest> {
    let rid = Uuid::new_v4();
    let pending = tunnel.register_pending(rid);
    let frame = RouterToNodeFrame::Request {
        rid,
        method: method.to_string(),
        path: path.to_string(),
        headers,
        body,
    };
    tunnel
        .send_frame(frame)
        .await
        .context("failed to send request frame to node")?;

    let _ = timeout;
    Ok(pending)
}

pub async fn forward_activate(
    tunnel: &NodeTunnel,
    session_id: &str,
    signature: &str,
    block_number: u64,
    message: Option<String>,
    timeout: Duration,
) -> Result<PendingRequest> {
    let rid = Uuid::new_v4();
    let pending = tunnel.register_pending(rid);
    let frame = RouterToNodeFrame::ActivateRequest {
        rid,
        session_id: session_id.to_string(),
        signature: signature.to_string(),
        block_number,
        message,
    };
    tunnel.send_frame(frame).await?;
    let _ = timeout;
    Ok(pending)
}
