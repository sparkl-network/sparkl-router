use std::collections::HashMap;

use serde::Serialize;
use tokio::sync::broadcast;

use crate::capacity::ModelCapacityKey;
use crate::node_auth::node_id_hex;
use crate::state::NodeId;

const TELEMETRY_CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TelemetryEvent {
    Snapshot {
        nodes: Vec<NodeStatusEvent>,
        models: Vec<ModelCapacityEvent>,
    },
    ModelCapacity(ModelCapacityEvent),
    NodeStatus(NodeStatusEvent),
}

#[derive(Debug, Clone, Serialize)]
pub struct ModelCapacityEvent {
    pub node_id: String,
    pub model_id: String,
    pub active_requests: u32,
    pub queued_requests: u32,
    pub concurrency: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeStatusEvent {
    pub node_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moniker: Option<String>,
    pub status: String,
    pub in_flight_requests: usize,
    pub model_count: i64,
}

#[derive(Clone)]
pub struct TelemetryBus {
    tx: broadcast::Sender<TelemetryEvent>,
}

impl Default for TelemetryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(TELEMETRY_CHANNEL_CAPACITY);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TelemetryEvent> {
        self.tx.subscribe()
    }

    pub fn emit(&self, event: TelemetryEvent) {
        let _ = self.tx.send(event);
    }

    pub fn emit_model_capacity(
        &self,
        key: &ModelCapacityKey,
        active_requests: u32,
        queued_requests: u32,
        concurrency: u32,
    ) {
        self.emit(TelemetryEvent::ModelCapacity(ModelCapacityEvent {
            node_id: node_id_hex(&key.node_id),
            model_id: key.model_id.clone(),
            active_requests,
            queued_requests,
            concurrency,
        }));
    }

    pub fn emit_node_status(
        &self,
        node_id: NodeId,
        moniker: Option<String>,
        status: &str,
        in_flight_requests: usize,
        model_count: i64,
    ) {
        self.emit(TelemetryEvent::NodeStatus(NodeStatusEvent {
            node_id: node_id_hex(&node_id),
            moniker,
            status: status.to_string(),
            in_flight_requests,
            model_count,
        }));
    }

    pub fn build_snapshot(
        node_events: Vec<NodeStatusEvent>,
        model_events: Vec<ModelCapacityEvent>,
    ) -> TelemetryEvent {
        TelemetryEvent::Snapshot {
            nodes: node_events,
            models: model_events,
        }
    }
}

pub fn model_capacity_events_from_tracker(
    tracker_snapshots: HashMap<ModelCapacityKey, (u32, u32)>,
    concurrency_lookup: impl Fn(&ModelCapacityKey) -> u32,
) -> Vec<ModelCapacityEvent> {
    let mut out: Vec<ModelCapacityEvent> = tracker_snapshots
        .into_iter()
        .map(|(key, (active, queued))| {
            let concurrency = concurrency_lookup(&key);
            ModelCapacityEvent {
                node_id: node_id_hex(&key.node_id),
                model_id: key.model_id,
                active_requests: active,
                queued_requests: queued,
                concurrency,
            }
        })
        .collect();
    out.sort_by(|a, b| a.model_id.cmp(&b.model_id).then(a.node_id.cmp(&b.node_id)));
    out
}
