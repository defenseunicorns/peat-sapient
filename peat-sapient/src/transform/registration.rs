//! `Registration` → `peat_schema::capability::v1::CapabilityAdvertisement`

use peat_schema::capability::v1::{
    Capability, CapabilityAdvertisement, CapabilityType, OperationalStatus,
};

use crate::proto::sapient_msg::bsi_flex_335_v2_0::{registration::NodeType, Registration};

fn node_type_to_capability_type(nt: NodeType) -> CapabilityType {
    match nt {
        NodeType::Radar
        | NodeType::Lidar
        | NodeType::Camera
        | NodeType::Seismic
        | NodeType::Acoustic
        | NodeType::ProximitySensor
        | NodeType::PassiveRf
        | NodeType::Chemical
        | NodeType::Biological
        | NodeType::Radiation => CapabilityType::Sensor,
        NodeType::MobileNode | NodeType::PointableNode => CapabilityType::Mobility,
        NodeType::Kinetic | NodeType::Ldew | NodeType::Rfdew | NodeType::Jammer => {
            CapabilityType::Payload
        }
        NodeType::Cyber => CapabilityType::Communication,
        NodeType::FusionNode => CapabilityType::Compute,
        NodeType::Human | NodeType::Other | NodeType::Unspecified => CapabilityType::Unspecified,
    }
}

/// Convert a SAPIENT `Registration` message to a peat-schema `CapabilityAdvertisement`.
///
/// `node_id` is taken from the outer `SapientMessage.node_id` field (UUID string).
pub fn from_registration(node_id: &str, msg: &Registration) -> CapabilityAdvertisement {
    let capabilities: Vec<Capability> = msg
        .node_definition
        .iter()
        .filter_map(|nd| {
            let nt_i32 = nd.node_type?;
            let nt = NodeType::try_from(nt_i32).ok()?;
            let cap_type = node_type_to_capability_type(nt);
            let type_name = format!("{nt:?}");

            let mut meta = serde_json::Map::new();
            if let Some(icd) = &msg.icd_version {
                meta.insert(
                    "sapient_icd_version".into(),
                    serde_json::Value::String(icd.clone()),
                );
            }
            if let Some(name) = &msg.name {
                meta.insert(
                    "sapient_name".into(),
                    serde_json::Value::String(name.clone()),
                );
            }
            if !nd.node_sub_type.is_empty() {
                meta.insert(
                    "sapient_node_sub_type".into(),
                    serde_json::Value::Array(
                        nd.node_sub_type
                            .iter()
                            .map(|s| serde_json::Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            if let Some(cd) = msg.config_data.first() {
                meta.insert(
                    "sapient_manufacturer".into(),
                    serde_json::Value::String(cd.manufacturer.clone()),
                );
                meta.insert(
                    "sapient_model".into(),
                    serde_json::Value::String(cd.model.clone()),
                );
            }

            Some(Capability {
                id: format!("{node_id}/{type_name}"),
                name: type_name,
                capability_type: cap_type as i32,
                confidence: 1.0,
                metadata_json: serde_json::to_string(&meta).unwrap_or_default(),
                registered_at: None,
            })
        })
        .collect();

    CapabilityAdvertisement {
        node_id: node_id.to_string(),
        advertised_at: None,
        capabilities,
        resources: None,
        operational_status: OperationalStatus::Ready as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::sapient_msg::bsi_flex_335_v2_0::registration::{
        ConfigurationData, NodeDefinition, NodeType,
    };

    fn make_reg(node_type: NodeType) -> Registration {
        Registration {
            node_definition: vec![NodeDefinition {
                node_type: Some(node_type as i32),
                node_sub_type: vec![],
            }],
            icd_version: Some("BSI Flex 335 v2.0".into()),
            name: Some("TestSensor".into()),
            capabilities: vec![],
            status_definition: None,
            mode_definition: vec![],
            dependent_nodes: vec![],
            reporting_region: vec![],
            config_data: vec![ConfigurationData {
                manufacturer: "Acme".into(),
                model: "Cam1000".into(),
                serial_number: Some("SN-001".into()),
                hardware_version: None,
                software_version: None,
                sub_components: vec![],
            }],
            short_name: None,
        }
    }

    #[test]
    fn camera_maps_to_sensor_capability() {
        let advert = from_registration("node-uuid-1", &make_reg(NodeType::Camera));
        assert_eq!(advert.capabilities.len(), 1);
        let cap = &advert.capabilities[0];
        assert_eq!(cap.capability_type, CapabilityType::Sensor as i32);
        assert_eq!(cap.name, "Camera");
    }

    #[test]
    fn radar_maps_to_sensor_capability() {
        let advert = from_registration("node-uuid-2", &make_reg(NodeType::Radar));
        assert_eq!(
            advert.capabilities[0].capability_type,
            CapabilityType::Sensor as i32
        );
    }

    #[test]
    fn mobile_node_maps_to_mobility_capability() {
        let advert = from_registration("node-uuid-3", &make_reg(NodeType::MobileNode));
        assert_eq!(
            advert.capabilities[0].capability_type,
            CapabilityType::Mobility as i32
        );
    }

    #[test]
    fn kinetic_maps_to_payload_capability() {
        let advert = from_registration("node-uuid-4", &make_reg(NodeType::Kinetic));
        assert_eq!(
            advert.capabilities[0].capability_type,
            CapabilityType::Payload as i32
        );
    }

    #[test]
    fn fusion_node_maps_to_compute_capability() {
        let advert = from_registration("node-uuid-5", &make_reg(NodeType::FusionNode));
        assert_eq!(
            advert.capabilities[0].capability_type,
            CapabilityType::Compute as i32
        );
    }

    #[test]
    fn node_id_preserved() {
        let advert = from_registration(
            "aaaabbbb-0000-1111-2222-333344445555",
            &make_reg(NodeType::Camera),
        );
        assert_eq!(advert.node_id, "aaaabbbb-0000-1111-2222-333344445555");
    }

    #[test]
    fn icd_version_in_metadata_json() {
        let advert = from_registration("node-1", &make_reg(NodeType::Camera));
        let meta: serde_json::Value =
            serde_json::from_str(&advert.capabilities[0].metadata_json).unwrap();
        assert_eq!(meta["sapient_icd_version"], "BSI Flex 335 v2.0");
    }

    #[test]
    fn manufacturer_and_model_in_metadata_json() {
        let advert = from_registration("node-1", &make_reg(NodeType::Camera));
        let meta: serde_json::Value =
            serde_json::from_str(&advert.capabilities[0].metadata_json).unwrap();
        assert_eq!(meta["sapient_manufacturer"], "Acme");
        assert_eq!(meta["sapient_model"], "Cam1000");
    }

    #[test]
    fn operational_status_is_ready() {
        let advert = from_registration("node-1", &make_reg(NodeType::Camera));
        assert_eq!(advert.operational_status, OperationalStatus::Ready as i32);
    }

    #[test]
    fn unknown_node_type_zero_maps_to_unspecified() {
        let advert = from_registration("node-1", &make_reg(NodeType::Unspecified));
        assert_eq!(
            advert.capabilities[0].capability_type,
            CapabilityType::Unspecified as i32
        );
    }

    #[test]
    fn multiple_node_definitions_produce_multiple_capabilities() {
        let mut reg = make_reg(NodeType::Camera);
        reg.node_definition.push(NodeDefinition {
            node_type: Some(NodeType::Radar as i32),
            node_sub_type: vec![],
        });
        let advert = from_registration("multi-node", &reg);
        assert_eq!(advert.capabilities.len(), 2);
    }
}
