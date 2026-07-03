//! Heartbeat-timeout watchdog for connected SAPIENT nodes.
//!
//! Wakes every `heartbeat_interval`, scans the registry, and emits
//! `SapientUpdate::NodeDisconnected` for any node that has been silent for
//! `2 × heartbeat_interval`. The node is removed from the registry before
//! the event is emitted.

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::bridge::SapientUpdate;
use crate::registry::{remove, NodeRegistry};

/// Run the heartbeat watchdog until `tx` is closed.
///
/// Checks every `heartbeat_interval`. Any node whose `last_seen` is older than
/// `2 × heartbeat_interval` is removed and a `NodeDisconnected` event is sent.
pub async fn run_watchdog(
    registry: NodeRegistry,
    heartbeat_interval: Duration,
    tx: mpsc::Sender<SapientUpdate>,
) {
    let mut ticker = time::interval(heartbeat_interval);
    ticker.tick().await; // burn the immediate first tick

    loop {
        ticker.tick().await;

        let threshold = heartbeat_interval * 2;
        let now = time::Instant::now();

        let expired: Vec<String> = {
            let map = registry.read().await;
            map.iter()
                .filter(|(_, n)| now.duration_since(n.last_seen) >= threshold)
                .map(|(k, _)| k.clone())
                .collect()
        };

        for node_id in expired {
            remove(&registry, &node_id).await;
            if tx
                .send(SapientUpdate::NodeDisconnected {
                    node_id: node_id.clone(),
                })
                .await
                .is_err()
            {
                return; // receiver dropped — shut down
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::SapientUpdate;
    use crate::registry::{get_node, new_registry, upsert};
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn silent_node_beyond_two_intervals_disconnects() {
        let registry = new_registry();
        let (tx, mut rx) = mpsc::channel(16);
        let interval = Duration::from_secs(5);

        upsert(&registry, "node-1", None, None).await;

        let reg = Arc::clone(&registry);
        tokio::spawn(run_watchdog(reg, interval, tx));

        // Let the watchdog consume the first tick (t=0, no expired nodes).
        tokio::task::yield_now().await;

        // Advance past 2× interval; ticks fire at t=interval and t=2×interval.
        tokio::time::advance(interval * 2 + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let update = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timed out waiting for NodeDisconnected")
            .expect("channel closed unexpectedly");

        assert!(
            matches!(&update, SapientUpdate::NodeDisconnected { node_id } if node_id == "node-1"),
            "unexpected update: {update:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn node_removed_from_registry_on_disconnect() {
        let registry = new_registry();
        let (tx, mut rx) = mpsc::channel(16);
        let interval = Duration::from_secs(5);

        upsert(&registry, "node-1", None, None).await;
        let reg = Arc::clone(&registry);
        tokio::spawn(run_watchdog(reg, interval, tx));

        tokio::task::yield_now().await;
        tokio::time::advance(interval * 2 + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let _ = rx.recv().await; // consume the NodeDisconnected event

        assert!(
            get_node(&registry, "node-1").await.is_none(),
            "node should be removed from registry after disconnect"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn active_node_is_not_disconnected() {
        let registry = new_registry();
        let (tx, mut rx) = mpsc::channel(16);
        let interval = Duration::from_secs(5);

        upsert(&registry, "node-1", None, None).await;
        let reg = Arc::clone(&registry);
        tokio::spawn(run_watchdog(Arc::clone(&reg), interval, tx));

        tokio::task::yield_now().await;

        // Advance 1.5× interval, then send a heartbeat (refresh last_seen)
        tokio::time::advance(interval * 3 / 2).await;
        tokio::task::yield_now().await;
        upsert(&reg, "node-1", None, None).await;

        // Advance another 1.5× interval (total 3×, but only 1.5× since last heartbeat)
        tokio::time::advance(interval * 3 / 2).await;
        tokio::task::yield_now().await;

        // Channel must be empty — active node should not have disconnected
        assert!(
            rx.try_recv().is_err(),
            "active node should not receive NodeDisconnected"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn only_silent_node_disconnects_when_multiple_nodes_present() {
        let registry = new_registry();
        let (tx, mut rx) = mpsc::channel(16);
        let interval = Duration::from_secs(5);

        upsert(&registry, "silent-node", None, None).await;
        upsert(&registry, "active-node", None, None).await;

        let reg = Arc::clone(&registry);
        tokio::spawn(run_watchdog(Arc::clone(&reg), interval, tx));

        tokio::task::yield_now().await;

        // At 1.5× interval, refresh active-node only
        tokio::time::advance(interval * 3 / 2).await;
        tokio::task::yield_now().await;
        upsert(&reg, "active-node", None, None).await;

        // Advance to 2× interval + epsilon (silent-node now expired)
        tokio::time::advance(interval / 2 + Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        let msg = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("timed out")
            .expect("channel closed");

        assert!(
            matches!(&msg, SapientUpdate::NodeDisconnected { node_id } if node_id == "silent-node"),
            "expected silent-node disconnect, got {msg:?}"
        );

        // active-node still present, no second event
        assert!(
            get_node(&registry, "active-node").await.is_some(),
            "active-node should still be in registry"
        );
        assert!(rx.try_recv().is_err(), "no second disconnect expected");
    }
}
