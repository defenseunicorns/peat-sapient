//! `StatusReport` → `(peat_schema::node::v1::NodeState, Option<CapabilityAdvertisement>)`

use peat_schema::{
    capability::v1::{CapabilityAdvertisement, OperationalStatus, ResourceStatus},
    common::v1::Position,
    node::v1::{HealthStatus, NodeState, Phase},
};

use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
    status_report::System, Location, LocationCoordinateSystem, StatusReport,
};

fn system_to_health(system_i32: i32) -> HealthStatus {
    match System::try_from(system_i32).unwrap_or(System::Unspecified) {
        System::Ok => HealthStatus::Nominal,
        System::Warning => HealthStatus::Degraded,
        System::Error => HealthStatus::Critical,
        System::Goodbye => HealthStatus::Failed,
        System::Unspecified => HealthStatus::Unspecified,
    }
}

fn system_to_operational_status(system_i32: i32) -> OperationalStatus {
    match System::try_from(system_i32).unwrap_or(System::Unspecified) {
        System::Ok => OperationalStatus::Ready,
        System::Warning => OperationalStatus::Degraded,
        System::Error | System::Goodbye => OperationalStatus::Offline,
        System::Unspecified => OperationalStatus::Unspecified,
    }
}

fn location_to_position(loc: &Location) -> Option<Position> {
    let x = loc.x?;
    let y = loc.y?;
    let cs = LocationCoordinateSystem::try_from(loc.coordinate_system.unwrap_or(0)).ok()?;
    match cs {
        LocationCoordinateSystem::LatLngDegM => Some(Position {
            latitude: y,
            longitude: x,
            altitude: loc.z.unwrap_or(0.0),
        }),
        LocationCoordinateSystem::LatLngRadM => Some(Position {
            latitude: y.to_degrees(),
            longitude: x.to_degrees(),
            altitude: loc.z.unwrap_or(0.0),
        }),
        // UTM and unspecified: delegate to caller (not resolvable from location alone without zone)
        _ => None,
    }
}

/// Convert a SAPIENT `StatusReport` to a `NodeState` plus an optional capability delta.
///
/// Returns `Some(CapabilityAdvertisement)` when the report carries FOV or mode data
/// that constitutes a capability change; `None` for a pure heartbeat.
pub fn from_status_report(
    node_id: &str,
    msg: &StatusReport,
) -> (NodeState, Option<CapabilityAdvertisement>) {
    let health = system_to_health(msg.system.unwrap_or(0));
    let position = msg.node_location.as_ref().and_then(location_to_position);

    // battery_percent stored in fuel_minutes — closest analog in NodeState until
    // peat-schema adds a dedicated battery field.
    let battery_pct = msg
        .power
        .as_ref()
        .and_then(|p| p.level)
        .unwrap_or(0)
        .clamp(0, 100) as u32;

    let state = NodeState {
        position,
        fuel_minutes: battery_pct,
        health: health as i32,
        phase: Phase::Hierarchy as i32,
        cell_id: None,
        zone_id: None,
        timestamp: None,
        kinematics: None,
        position_error: None,
    };

    // Emit a capability delta whenever FOV or mode is present.
    let capability_delta = if msg.field_of_view.is_some() || msg.mode.is_some() {
        let battery_f = battery_pct as f32 / 100.0;
        let op_status = system_to_operational_status(msg.system.unwrap_or(0));
        Some(CapabilityAdvertisement {
            node_id: node_id.to_string(),
            advertised_at: None,
            capabilities: vec![],
            resources: Some(ResourceStatus {
                compute_utilization: 0.0,
                memory_utilization: 0.0,
                power_level: battery_f,
                storage_utilization: 0.0,
                bandwidth_utilization: 0.0,
                extra_json: String::new(),
            }),
            operational_status: op_status as i32,
        })
    } else {
        None
    };

    (state, capability_delta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
        status_report::{Power, System},
        Location, LocationCoordinateSystem, LocationDatum, StatusReport,
    };

    fn bare_report(system: System) -> StatusReport {
        StatusReport {
            system: Some(system as i32),
            ..Default::default()
        }
    }

    #[test]
    fn system_ok_maps_to_nominal() {
        let (state, _) = from_status_report("n1", &bare_report(System::Ok));
        assert_eq!(state.health, HealthStatus::Nominal as i32);
    }

    #[test]
    fn system_warning_maps_to_degraded() {
        let (state, _) = from_status_report("n1", &bare_report(System::Warning));
        assert_eq!(state.health, HealthStatus::Degraded as i32);
    }

    #[test]
    fn system_error_maps_to_critical() {
        let (state, _) = from_status_report("n1", &bare_report(System::Error));
        assert_eq!(state.health, HealthStatus::Critical as i32);
    }

    #[test]
    fn system_goodbye_maps_to_failed() {
        let (state, _) = from_status_report("n1", &bare_report(System::Goodbye));
        assert_eq!(state.health, HealthStatus::Failed as i32);
    }

    #[test]
    fn battery_level_stored_in_fuel_minutes() {
        let report = StatusReport {
            system: Some(System::Ok as i32),
            power: Some(Power {
                level: Some(85),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (state, _) = from_status_report("n1", &report);
        assert_eq!(state.fuel_minutes, 85);
    }

    #[test]
    fn battery_clamped_to_100() {
        let report = StatusReport {
            system: Some(System::Ok as i32),
            power: Some(Power {
                level: Some(150),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (state, _) = from_status_report("n1", &report);
        assert_eq!(state.fuel_minutes, 100);
    }

    #[test]
    fn node_location_lat_lng_deg_maps_to_position() {
        let report = StatusReport {
            system: Some(System::Ok as i32),
            node_location: Some(Location {
                x: Some(-0.1278), // longitude
                y: Some(51.5074), // latitude
                z: Some(11.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (state, _) = from_status_report("n1", &report);
        let pos = state.position.unwrap();
        assert!((pos.latitude - 51.5074).abs() < 1e-6);
        assert!((pos.longitude - (-0.1278)).abs() < 1e-6);
        assert!((pos.altitude - 11.0).abs() < 1e-6);
    }

    #[test]
    fn node_location_radians_converts_to_degrees() {
        let lat_deg = 51.5074_f64;
        let lon_deg = -0.1278_f64;
        let report = StatusReport {
            system: Some(System::Ok as i32),
            node_location: Some(Location {
                x: Some(lon_deg.to_radians()),
                y: Some(lat_deg.to_radians()),
                z: Some(0.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngRadM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (state, _) = from_status_report("n1", &report);
        let pos = state.position.unwrap();
        assert!((pos.latitude - lat_deg).abs() < 1e-4);
        assert!((pos.longitude - lon_deg).abs() < 1e-4);
    }

    #[test]
    fn no_location_yields_none_position() {
        let (state, _) = from_status_report("n1", &bare_report(System::Ok));
        assert!(state.position.is_none());
    }

    #[test]
    fn fov_present_yields_capability_delta() {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
            location_or_range_bearing::FovOneof, LocationList, LocationOrRangeBearing,
        };
        let report = StatusReport {
            system: Some(System::Ok as i32),
            field_of_view: Some(LocationOrRangeBearing {
                fov_oneof: Some(FovOneof::LocationList(LocationList { locations: vec![] })),
            }),
            ..Default::default()
        };
        let (_, delta) = from_status_report("n1", &report);
        assert!(delta.is_some());
    }

    #[test]
    fn mode_present_yields_capability_delta() {
        let report = StatusReport {
            system: Some(System::Ok as i32),
            mode: Some("scanning".into()),
            ..Default::default()
        };
        let (_, delta) = from_status_report("n1", &report);
        assert!(delta.is_some());
    }

    #[test]
    fn pure_heartbeat_yields_no_delta() {
        let (_, delta) = from_status_report("n1", &bare_report(System::Ok));
        assert!(delta.is_none());
    }

    #[test]
    fn capability_delta_carries_battery_level() {
        let report = StatusReport {
            system: Some(System::Ok as i32),
            power: Some(Power {
                level: Some(72),
                ..Default::default()
            }),
            mode: Some("default".into()),
            ..Default::default()
        };
        let (_, delta) = from_status_report("n1", &report);
        let resources = delta.unwrap().resources.unwrap();
        assert!((resources.power_level - 0.72).abs() < 1e-4);
    }
}
