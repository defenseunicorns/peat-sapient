//! Bridge → mesh subscriber: consumes [`SapientUpdate`]s from a
//! [`SapientBridge`] and publishes the corresponding documents to a
//! [`peat_mesh::Node`].
//!
//! This is the glue between the two integration surfaces in this crate:
//! `SapientBridge` handles C2 (tasking, ack, watchdog) and owns the TCP
//! connections; this subscriber takes the bridge's update stream and
//! projects it into the mesh's `tracks` and `platforms` collections using
//! the same [`mesh_fields`] projections that [`SapientTranslator`] uses.
//!
//! [`SapientUpdate`]: peat_sapient::bridge::SapientUpdate
//! [`SapientBridge`]: peat_sapient::bridge::SapientBridge
//! [`mesh_fields`]: peat_sapient::mesh_fields
//! [`SapientTranslator`]: crate::SapientTranslator

use std::sync::Arc;

use peat_mesh::sync::types::Document as MeshDocument;
use peat_mesh::Node;
use peat_sapient::bridge::SapientUpdate;
use peat_sapient::mesh_fields::{platform_to_fields, track_to_fields};
use peat_schema::capability::v1::CapabilityAdvertisement;
use tokio::sync::mpsc;
use tracing::warn;

const TRACKS_COLLECTION: &str = "tracks";
const PLATFORMS_COLLECTION: &str = "platforms";
const SAPIENT_ORIGIN: &str = "sapient";

/// Consume a [`SapientUpdate`] stream and publish documents to a mesh
/// [`Node`]. Runs until the sender half is dropped.
///
/// Publishes:
/// - `Registered` → `platforms` (from `CapabilityAdvertisement`)
/// - `StatusUpdated` → `platforms` (from `CapabilityAdvertisement` + `NodeState`)
/// - `Detected` → `tracks` (from `Track`)
///
/// C2 updates (`TaskAcknowledged`, `Alerted`, `AlertAcknowledged`) and
/// lifecycle events (`NodeDisconnected`, `Ignored`) are not published —
/// they have no mesh document representation in v1.
pub async fn run_bridge_subscriber(mut rx: mpsc::Receiver<SapientUpdate>, node: Arc<Node>) {
    while let Some(update) = rx.recv().await {
        let result = match update {
            SapientUpdate::Registered { advertisement, .. } => {
                let (id, fields) = platform_to_fields(&advertisement, None);
                let doc = MeshDocument::with_id(id, fields.into_iter().collect());
                node.publish_with_origin(PLATFORMS_COLLECTION, doc, Some(SAPIENT_ORIGIN.into()))
                    .await
            }

            SapientUpdate::StatusUpdated {
                ref node_id,
                ref state,
                ref capability_delta,
            } => {
                let advertisement = capability_delta.clone().unwrap_or(CapabilityAdvertisement {
                    node_id: node_id.clone(),
                    ..Default::default()
                });
                let (id, fields) = platform_to_fields(&advertisement, Some(state));
                let doc = MeshDocument::with_id(id, fields.into_iter().collect());
                node.publish_with_origin(PLATFORMS_COLLECTION, doc, Some(SAPIENT_ORIGIN.into()))
                    .await
            }

            SapientUpdate::Detected { ref track, .. } => {
                let (id, fields) = track_to_fields(track);
                let doc = MeshDocument::with_id(id, fields.into_iter().collect());
                node.publish_with_origin(TRACKS_COLLECTION, doc, Some(SAPIENT_ORIGIN.into()))
                    .await
            }

            _ => continue,
        };

        if let Err(err) = result {
            warn!(%err, "sapient bridge subscriber: publish failed");
        }
    }
}
