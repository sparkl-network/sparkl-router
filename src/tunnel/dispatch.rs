use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::Value;
use uuid::Uuid;

use crate::protocol::{InboundFrame, RouterToNodeFrame};
use crate::state::NodeTunnel;

pub struct PendingRequest {
    pub rx: tokio::sync::mpsc::Receiver<InboundFrame>,
    _cleanup: PendingCleanup,
}

struct PendingCleanup {
    tunnel: Arc<NodeTunnel>,
    rid: Uuid,
}

impl Drop for PendingCleanup {
    fn drop(&mut self) {
        self.tunnel.pending.remove(&self.rid);
    }
}

impl NodeTunnel {
    pub fn register_pending(self: &Arc<Self>, rid: Uuid) -> PendingRequest {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        self.pending.insert(rid, tx);
        PendingRequest {
            rx,
            _cleanup: PendingCleanup {
                tunnel: Arc::clone(self),
                rid,
            },
        }
    }

    pub fn route_inbound(&self, rid: Uuid, frame: InboundFrame) {
        if is_terminal_frame(&frame) {
            if let Some((_, tx)) = self.pending.remove(&rid) {
                let _ = tx.try_send(frame);
            }
            return;
        }

        let send_failed = if let Some(tx) = self.pending.get(&rid) {
            tx.try_send(frame).is_err()
        } else {
            false
        };

        if send_failed {
            self.pending.remove(&rid);
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

fn is_terminal_frame(frame: &InboundFrame) -> bool {
    matches!(
        frame,
        InboundFrame::End { .. }
            | InboundFrame::Error { .. }
            | InboundFrame::ActivateResponse { .. }
    )
}

pub async fn forward_http_request(
    tunnel: &Arc<NodeTunnel>,
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
    tunnel: &Arc<NodeTunnel>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_entry_removed_when_request_dropped_without_terminal_frame() {
        let (frame_tx, _frame_rx) = tokio::sync::mpsc::channel(8);
        let (shutdown_tx, _shutdown_rx) = tokio::sync::mpsc::channel(1);
        let tunnel = Arc::new(NodeTunnel::new(
            [1u8; 32],
            None,
            frame_tx,
            shutdown_tx,
        ));
        let rid = Uuid::new_v4();

        let pending = tunnel.register_pending(rid);
        assert_eq!(tunnel.in_flight_count(), 1);

        drop(pending);
        assert_eq!(tunnel.in_flight_count(), 0);
    }
}
