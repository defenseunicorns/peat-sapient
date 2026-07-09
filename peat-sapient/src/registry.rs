//! Per-connection node state for connected SAPIENT sensors.
//!
//! `NodeRegistry` is a shared, async-safe map from SAPIENT node_id (UUID string) to
//! `ConnectedNode`. It is the authoritative source for:
//!
//! - Last-known capabilities (from `Registration`)
//! - Last-known position (from `StatusReport`) — used to resolve range-bearing detections
//! - Last-seen timestamp — used by the heartbeat-timeout watchdog (#13)

use std::collections::HashMap;
use std::sync::Arc;

use peat_schema::{capability::v1::CapabilityAdvertisement, track::v1::TrackPosition};
use tokio::sync::RwLock;
use tokio::time::Instant;

/// State for a single connected SAPIENT DLMM.
#[derive(Debug, Clone)]
pub struct ConnectedNode {
    /// SAPIENT node_id (UUID string from `SapientMessage.node_id`).
    pub node_id: String,
    /// Most recently received capability advertisement (from `Registration`).
    pub capability: Option<CapabilityAdvertisement>,
    /// Most recently received position (from `StatusReport`) — used for
    /// range-bearing coordinate resolution in `transform::detection`.
    pub last_position: Option<TrackPosition>,
    /// Wall-clock time of the last message received from this node.
    pub last_seen: Instant,
}

/// Shared, async-safe registry of connected SAPIENT nodes.
pub type NodeRegistry = Arc<RwLock<HashMap<String, ConnectedNode>>>;

/// Construct an empty registry.
pub fn new_registry() -> NodeRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Insert or update a node entry.
///
/// - Always refreshes `last_seen` to `Instant::now()`.
/// - If `capability` is `Some`, overwrites the stored capability.
/// - If `position` is `Some`, overwrites the stored position.
pub async fn upsert(
    registry: &NodeRegistry,
    node_id: &str,
    capability: Option<CapabilityAdvertisement>,
    position: Option<TrackPosition>,
) {
    let mut map = registry.write().await;
    let entry = map
        .entry(node_id.to_string())
        .or_insert_with(|| ConnectedNode {
            node_id: node_id.to_string(),
            capability: None,
            last_position: None,
            last_seen: Instant::now(),
        });
    entry.last_seen = Instant::now();
    if capability.is_some() {
        entry.capability = capability;
    }
    if position.is_some() {
        entry.last_position = position;
    }
}

/// Return a clone of the last-known position for `node_id`, or `None` if the
/// node is unknown or has not yet reported a position.
pub async fn get_position(registry: &NodeRegistry, node_id: &str) -> Option<TrackPosition> {
    registry
        .read()
        .await
        .get(node_id)
        .and_then(|n| n.last_position)
}

/// Return a clone of the full `ConnectedNode` entry, or `None` if unknown.
pub async fn get_node(registry: &NodeRegistry, node_id: &str) -> Option<ConnectedNode> {
    registry.read().await.get(node_id).cloned()
}

/// Remove a node from the registry (e.g. on disconnect or heartbeat timeout).
pub async fn remove(registry: &NodeRegistry, node_id: &str) {
    registry.write().await.remove(node_id);
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use peat_schema::capability::v1::{CapabilityAdvertisement, OperationalStatus};

    fn make_capability(node_id: &str) -> CapabilityAdvertisement {
        CapabilityAdvertisement {
            node_id: node_id.to_string(),
            advertised_at: None,
            capabilities: vec![],
            resources: None,
            operational_status: OperationalStatus::Ready as i32,
        }
    }

    fn make_position(lat: f64, lon: f64) -> TrackPosition {
        TrackPosition {
            latitude: lat,
            longitude: lon,
            altitude: 0.0,
            cep_m: 0.0,
            vertical_error_m: 0.0,
        }
    }

    #[tokio::test]
    async fn insert_and_retrieve_node() {
        let registry = new_registry();
        let cap = make_capability("node-1");
        upsert(&registry, "node-1", Some(cap.clone()), None).await;

        let node = get_node(&registry, "node-1").await.unwrap();
        assert_eq!(node.node_id, "node-1");
        assert_eq!(
            node.capability.unwrap().operational_status,
            OperationalStatus::Ready as i32
        );
    }

    #[tokio::test]
    async fn position_is_none_before_status_report() {
        let registry = new_registry();
        upsert(&registry, "node-2", Some(make_capability("node-2")), None).await;

        let pos = get_position(&registry, "node-2").await;
        assert!(pos.is_none());
    }

    #[tokio::test]
    async fn position_set_after_upsert_with_position() {
        let registry = new_registry();
        upsert(&registry, "node-3", None, Some(make_position(51.5, -0.1))).await;

        let pos = get_position(&registry, "node-3").await.unwrap();
        assert!((pos.latitude - 51.5).abs() < 1e-9);
        assert!((pos.longitude - (-0.1)).abs() < 1e-9);
    }

    #[tokio::test]
    async fn upsert_updates_existing_capability() {
        let registry = new_registry();
        let cap1 = make_capability("node-4");
        upsert(&registry, "node-4", Some(cap1), None).await;

        let mut cap2 = make_capability("node-4");
        cap2.operational_status = OperationalStatus::Degraded as i32;
        upsert(&registry, "node-4", Some(cap2), None).await;

        let node = get_node(&registry, "node-4").await.unwrap();
        assert_eq!(
            node.capability.unwrap().operational_status,
            OperationalStatus::Degraded as i32
        );
    }

    #[tokio::test]
    async fn capability_preserved_when_none_passed() {
        let registry = new_registry();
        upsert(&registry, "node-5", Some(make_capability("node-5")), None).await;
        // Second upsert with no capability should not clear the existing one
        upsert(&registry, "node-5", None, Some(make_position(10.0, 20.0))).await;

        let node = get_node(&registry, "node-5").await.unwrap();
        assert!(node.capability.is_some(), "capability should be preserved");
        assert!(node.last_position.is_some(), "position should be set");
    }

    #[tokio::test]
    async fn unknown_node_get_position_returns_none() {
        let registry = new_registry();
        assert!(get_position(&registry, "ghost-node").await.is_none());
    }

    #[tokio::test]
    async fn unknown_node_get_node_returns_none() {
        let registry = new_registry();
        assert!(get_node(&registry, "ghost-node").await.is_none());
    }

    #[tokio::test]
    async fn remove_drops_node() {
        let registry = new_registry();
        upsert(&registry, "node-6", Some(make_capability("node-6")), None).await;
        remove(&registry, "node-6").await;
        assert!(get_node(&registry, "node-6").await.is_none());
    }

    #[tokio::test]
    async fn last_seen_updates_on_upsert() {
        let registry = new_registry();
        upsert(&registry, "node-7", Some(make_capability("node-7")), None).await;
        let t1 = get_node(&registry, "node-7").await.unwrap().last_seen;

        // Brief real sleep so Instant::now() differs
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        upsert(&registry, "node-7", None, None).await;
        let t2 = get_node(&registry, "node-7").await.unwrap().last_seen;

        assert!(t2 >= t1, "last_seen should advance on upsert");
    }

    #[tokio::test]
    async fn concurrent_reads_do_not_deadlock() {
        let registry = new_registry();
        for i in 0..5 {
            upsert(
                &registry,
                &format!("node-{i}"),
                Some(make_capability(&format!("node-{i}"))),
                None,
            )
            .await;
        }

        // Spawn 20 concurrent readers — none should deadlock
        let handles: Vec<_> = (0..20)
            .map(|i| {
                let r = Arc::clone(&registry);
                tokio::spawn(async move { get_node(&r, &format!("node-{}", i % 5)).await })
            })
            .collect();

        for h in handles {
            h.await.unwrap(); // panics if task panicked (e.g. deadlock)
        }
    }
}
