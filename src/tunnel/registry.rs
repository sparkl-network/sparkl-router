use std::sync::Arc;

use dashmap::DashMap;

use crate::state::{NodeId, NodeTunnel};

#[derive(Clone, Default)]
pub struct TunnelRegistry {
    inner: Arc<DashMap<NodeId, Arc<NodeTunnel>>>,
}

impl TunnelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, node_id: NodeId, tunnel: Arc<NodeTunnel>) -> Option<Arc<NodeTunnel>> {
        self.inner.insert(node_id, tunnel)
    }

    pub fn get(&self, node_id: &NodeId) -> Option<Arc<NodeTunnel>> {
        self.inner.get(node_id).map(|e| e.clone())
    }

    pub fn remove(&self, node_id: &NodeId) -> Option<Arc<NodeTunnel>> {
        self.inner.remove(node_id).map(|(_, t)| t)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (NodeId, Arc<NodeTunnel>)> + '_ {
        self.inner
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
    }
}
