//! `DetectionReport` → `peat_schema::track::v1::Track`
//!
//! Coordinate systems handled:
//!
//! **Location** (`LocationCoordinateSystem`):
//! - `LatLngDegM` (1) — lat/lon degrees, altitude metres; passthrough
//! - `LatLngRadM` (2) — lat/lon radians, altitude metres; angles converted to degrees
//! - raw 3 (deprecated SAPIENT v7: degrees/feet) — altitude converted to metres
//! - raw 4 (deprecated SAPIENT v7: radians/feet) — angles to degrees, altitude to metres
//! - `UtmM` (5) — UTM metres; inverse Transverse Mercator projection to WGS84
//! - `x_error`/`y_error` → `position_error.circular_error` for all variants (1-σ errors in coordinate units)
//!
//! **RangeBearing** (`RangeBearingCoordinateSystem`):
//! - `DegreesM` (1) — azimuth/elevation in degrees, range in metres; passthrough
//! - `RadiansM` (2) — azimuth/elevation in radians; converted to degrees
//! - `DegreesKm` (3) — range in km; converted to metres
//! - `RadiansKm` (4) — angles in radians + range in km; both normalised
//! - raw 5 (deprecated SAPIENT v7: degrees/feet) — range converted to metres
//! - raw 6 (deprecated SAPIENT v7: radians/feet) — angles to degrees, range to metres
//! - Requires sensor position; returns `Err(UnsupportedCoordinateSystem)` when `None`.
//!
//! Note: MGRS is not a `LocationCoordinateSystem` variant in BSI Flex 335 v2.0.
//! ADR-070 anticipated it, but the vendored proto defines only the variants above.

use peat_schema::common::v1::PositionError;
use peat_schema::track::v1::{SourceType, Track, TrackPosition, TrackSource, TrackState, Velocity};

use crate::{
    error::SapientError,
    proto::sapient_msg::bsi_flex_335_v2_0::{
        detection_report::{LocationOneof, VelocityOneof},
        DetectionReport, EnuVelocity, Location, LocationCoordinateSystem, RangeBearing,
        RangeBearingCoordinateSystem,
    },
};

const FEET_TO_METRES: f64 = 0.3048;
const KM_TO_METRES: f64 = 1_000.0;

// ── Coordinate helpers ────────────────────────────────────────────────────────

/// 50% CEP from two independent 1-σ errors in metres.
/// Approximation: CEP ≈ 0.5887 × (σ_x + σ_y); exact when σ_x = σ_y.
fn cep_from_errors_m(sigma_x_m: f64, sigma_y_m: f64) -> f32 {
    (0.5887 * (sigma_x_m + sigma_y_m)) as f32
}

/// Convert a Location's 1-σ x/y errors (in coordinate-system units) to metres.
/// Returns (sigma_x_m, sigma_y_m).
fn location_errors_to_m(x_err: f64, y_err: f64, lat_deg: f64, raw_cs: i32) -> (f64, f64) {
    let lat_rad = lat_deg.to_radians();
    match raw_cs {
        // Degrees (LatLngDegM or deprecated value 3): errors in degrees
        1 | 3 => (
            x_err.to_radians() * WGS84_A * lat_rad.cos(),
            y_err.to_radians() * WGS84_A,
        ),
        // Radians (LatLngRadM or deprecated value 4): errors in radians
        2 | 4 => (x_err * WGS84_A * lat_rad.cos(), y_err * WGS84_A),
        // UTM (UtmM): errors already in metres
        5 => (x_err, y_err),
        _ => (0.0, 0.0),
    }
}

/// Normalise a `RangeBearing` message to (range_m, azimuth_deg, elevation_deg,
/// range_error_m), handling all `RangeBearingCoordinateSystem` variants including
/// the deprecated SAPIENT v7 feet variants (raw values 5 and 6).
fn normalize_rb(rb: &RangeBearing) -> Result<(f64, f64, f64, f64), SapientError> {
    let range_raw = rb.range.ok_or_else(|| SapientError::MappingError {
        kind: "range_bearing",
        detail: "RangeBearing.range missing".into(),
    })?;
    let az_raw = rb.azimuth.ok_or_else(|| SapientError::MappingError {
        kind: "range_bearing",
        detail: "RangeBearing.azimuth missing".into(),
    })?;
    let el_raw = rb.elevation.unwrap_or(0.0);
    let range_err_raw = rb.range_error.unwrap_or(0.0);

    let raw_cs = rb.coordinate_system.unwrap_or(0);

    // Deprecated from SAPIENT v7: raw 5 (degrees/feet) and 6 (radians/feet).
    if raw_cs == 5 {
        return Ok((
            range_raw * FEET_TO_METRES,
            az_raw,
            el_raw,
            range_err_raw * FEET_TO_METRES,
        ));
    }
    if raw_cs == 6 {
        return Ok((
            range_raw * FEET_TO_METRES,
            az_raw.to_degrees(),
            el_raw.to_degrees(),
            range_err_raw * FEET_TO_METRES,
        ));
    }

    let cs = RangeBearingCoordinateSystem::try_from(raw_cs)
        .unwrap_or(RangeBearingCoordinateSystem::Unspecified);

    match cs {
        RangeBearingCoordinateSystem::DegreesM => Ok((range_raw, az_raw, el_raw, range_err_raw)),
        RangeBearingCoordinateSystem::RadiansM => Ok((
            range_raw,
            az_raw.to_degrees(),
            el_raw.to_degrees(),
            range_err_raw,
        )),
        RangeBearingCoordinateSystem::DegreesKm => Ok((
            range_raw * KM_TO_METRES,
            az_raw,
            el_raw,
            range_err_raw * KM_TO_METRES,
        )),
        RangeBearingCoordinateSystem::RadiansKm => Ok((
            range_raw * KM_TO_METRES,
            az_raw.to_degrees(),
            el_raw.to_degrees(),
            range_err_raw * KM_TO_METRES,
        )),
        RangeBearingCoordinateSystem::Unspecified => {
            Err(SapientError::UnsupportedCoordinateSystem(
                "unspecified range-bearing coordinate system".into(),
            ))
        }
    }
}

// ── UTM → WGS84 (Transverse Mercator inverse, WGS84 ellipsoid) ───────────────

const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257_223_563;
const WGS84_E2: f64 = 2.0 * WGS84_F - WGS84_F * WGS84_F;
const WGS84_E_PRIME2: f64 = WGS84_E2 / (1.0 - WGS84_E2);
const UTM_K0: f64 = 0.9996;

fn utm_to_latlon(easting: f64, northing: f64, zone_number: u8, northern: bool) -> (f64, f64) {
    let x = easting - 500_000.0;
    let y = if northern {
        northing
    } else {
        northing - 10_000_000.0
    };
    let lon0_rad = ((zone_number as f64 - 1.0) * 6.0 - 180.0 + 3.0).to_radians();

    let m = y / UTM_K0;
    let mu = m
        / (WGS84_A
            * (1.0
                - WGS84_E2 / 4.0
                - 3.0 * WGS84_E2.powi(2) / 64.0
                - 5.0 * WGS84_E2.powi(3) / 256.0));

    let e1 = (1.0 - (1.0 - WGS84_E2).sqrt()) / (1.0 + (1.0 - WGS84_E2).sqrt());

    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1.powi(2) / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1.powi(4) / 512.0) * (8.0 * mu).sin();

    let sin_phi1 = phi1.sin();
    let cos_phi1 = phi1.cos();
    let tan_phi1 = phi1.tan();
    let n1 = WGS84_A / (1.0 - WGS84_E2 * sin_phi1.powi(2)).sqrt();
    let t1 = tan_phi1.powi(2);
    let c1 = WGS84_E_PRIME2 * cos_phi1.powi(2);
    let r1 = WGS84_A * (1.0 - WGS84_E2) / (1.0 - WGS84_E2 * sin_phi1.powi(2)).powf(1.5);
    let d = x / (n1 * UTM_K0);

    let lat_rad = phi1
        - (n1 * tan_phi1 / r1)
            * (d.powi(2) / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1.powi(2) - 9.0 * WGS84_E_PRIME2)
                    * d.powi(4)
                    / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1.powi(2)
                    - 252.0 * WGS84_E_PRIME2
                    - 3.0 * c1.powi(2))
                    * d.powi(6)
                    / 720.0);

    let lon_rad = lon0_rad
        + (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1.powi(2)
                + 8.0 * WGS84_E_PRIME2
                + 24.0 * t1.powi(2))
                * d.powi(5)
                / 120.0)
            / cos_phi1;

    (lat_rad.to_degrees(), lon_rad.to_degrees())
}

/// Parse a UTM zone string like "30U" or "30N" into `(zone_number, is_northern)`.
fn parse_utm_zone(zone: &str) -> Option<(u8, bool)> {
    let s = zone.trim();
    if s.is_empty() {
        return None;
    }
    let letter = s.chars().last()?;
    let num_part = &s[..s.len() - letter.len_utf8()];
    let zone_num: u8 = num_part.parse().ok()?;
    if !(1..=60).contains(&zone_num) {
        return None;
    }
    let northern = matches!(
        letter.to_ascii_uppercase(),
        'N' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X'
    );
    Some((zone_num, northern))
}

// ── Location conversion ───────────────────────────────────────────────────────

#[allow(deprecated)]
pub(crate) fn location_to_track_position(
    loc: &Location,
) -> Result<(TrackPosition, PositionError), SapientError> {
    let x = loc.x.ok_or_else(|| SapientError::MappingError {
        kind: "location",
        detail: "Location.x missing".into(),
    })?;
    let y = loc.y.ok_or_else(|| SapientError::MappingError {
        kind: "location",
        detail: "Location.y missing".into(),
    })?;
    let z = loc.z.unwrap_or(0.0);
    let x_err = loc.x_error.unwrap_or(0.0);
    let y_err = loc.y_error.unwrap_or(0.0);
    let z_err = loc.z_error.unwrap_or(0.0);

    let raw_cs = loc.coordinate_system.unwrap_or(0);

    // Raw values 3 and 4 are reserved/removed from the BSI Flex 335 v2.0 enum
    // (deprecated from SAPIENT v7: degrees/feet and radians/feet). Legacy sensors
    // may still emit them; handle before the enum conversion.
    if raw_cs == 3 {
        let (sx, sy) = location_errors_to_m(x_err, y_err, y, raw_cs);
        let cep = cep_from_errors_m(sx, sy);
        let vert = (z_err * FEET_TO_METRES) as f32;
        return Ok((
            TrackPosition {
                latitude: y,
                longitude: x,
                altitude: (z * FEET_TO_METRES) as f32,
                cep_m: cep,
                vertical_error_m: vert,
            },
            PositionError { circular_error: cep, linear_error: 0.0, vertical_error: vert },
        ));
    }
    if raw_cs == 4 {
        let lat_deg = y.to_degrees();
        let (sx, sy) = location_errors_to_m(x_err, y_err, lat_deg, raw_cs);
        let cep = cep_from_errors_m(sx, sy);
        let vert = (z_err * FEET_TO_METRES) as f32;
        return Ok((
            TrackPosition {
                latitude: lat_deg,
                longitude: x.to_degrees(),
                altitude: (z * FEET_TO_METRES) as f32,
                cep_m: cep,
                vertical_error_m: vert,
            },
            PositionError { circular_error: cep, linear_error: 0.0, vertical_error: vert },
        ));
    }

    let cs =
        LocationCoordinateSystem::try_from(raw_cs).unwrap_or(LocationCoordinateSystem::Unspecified);

    match cs {
        LocationCoordinateSystem::LatLngDegM => {
            let (sx, sy) = location_errors_to_m(x_err, y_err, y, raw_cs);
            let cep = cep_from_errors_m(sx, sy);
            let vert = z_err as f32;
            Ok((
                TrackPosition {
                    latitude: y,
                    longitude: x,
                    altitude: z as f32,
                    cep_m: cep,
                    vertical_error_m: vert,
                },
                PositionError { circular_error: cep, linear_error: 0.0, vertical_error: vert },
            ))
        }
        LocationCoordinateSystem::LatLngRadM => {
            let lat_deg = y.to_degrees();
            let (sx, sy) = location_errors_to_m(x_err, y_err, lat_deg, raw_cs);
            let cep = cep_from_errors_m(sx, sy);
            let vert = z_err as f32;
            Ok((
                TrackPosition {
                    latitude: lat_deg,
                    longitude: x.to_degrees(),
                    altitude: z as f32,
                    cep_m: cep,
                    vertical_error_m: vert,
                },
                PositionError { circular_error: cep, linear_error: 0.0, vertical_error: vert },
            ))
        }
        LocationCoordinateSystem::UtmM => {
            let zone_str = loc
                .utm_zone
                .as_deref()
                .ok_or_else(|| SapientError::MappingError {
                    kind: "location",
                    detail: "UTM location missing utm_zone".into(),
                })?;
            let (zone_num, northern) =
                parse_utm_zone(zone_str).ok_or_else(|| SapientError::MappingError {
                    kind: "location",
                    detail: format!("invalid utm_zone: {zone_str}"),
                })?;
            let (lat, lon) = utm_to_latlon(x, y, zone_num, northern);
            let (sx, sy) = location_errors_to_m(x_err, y_err, lat, raw_cs);
            let cep = cep_from_errors_m(sx, sy);
            let vert = z_err as f32;
            Ok((
                TrackPosition {
                    latitude: lat,
                    longitude: lon,
                    altitude: z as f32,
                    cep_m: cep,
                    vertical_error_m: vert,
                },
                PositionError { circular_error: cep, linear_error: 0.0, vertical_error: vert },
            ))
        }
        LocationCoordinateSystem::Unspecified => Err(SapientError::UnsupportedCoordinateSystem(
            "unspecified coordinate system".into(),
        )),
    }
}

#[allow(deprecated)]
fn range_bearing_to_track_position(
    rb: &RangeBearing,
    sensor: &TrackPosition,
) -> Result<(TrackPosition, PositionError), SapientError> {
    let (range, azimuth_deg, elevation_deg, range_err) = normalize_rb(rb)?;

    // Convert spherical (range, azimuth, elevation) to Cartesian offset, then
    // add to sensor lat/lon using the small-angle flat-earth approximation.
    // Valid for ranges < ~50 km where curvature is negligible.
    let az_rad = azimuth_deg.to_radians();
    let el_rad = elevation_deg.to_radians();
    let horiz = range * el_rad.cos();
    let north_m = horiz * az_rad.cos();
    let east_m = horiz * az_rad.sin();
    let up_m = range * el_rad.sin();

    // 1° latitude ≈ 111_111 m; 1° longitude ≈ 111_111 * cos(lat) m
    let lat_deg_per_m = 1.0 / 111_111.0;
    let lon_deg_per_m = 1.0 / (111_111.0 * sensor.latitude.to_radians().cos());

    let cep = range_err as f32;
    Ok((
        TrackPosition {
            latitude: sensor.latitude + north_m * lat_deg_per_m,
            longitude: sensor.longitude + east_m * lon_deg_per_m,
            altitude: sensor.altitude + up_m as f32,
            cep_m: cep,
            vertical_error_m: 0.0,
        },
        PositionError { circular_error: cep, linear_error: 0.0, vertical_error: 0.0 },
    ))
}

// ── Velocity conversion ───────────────────────────────────────────────────────

fn enu_to_velocity(enu: &EnuVelocity) -> Velocity {
    let east = enu.east_rate.unwrap_or(0.0);
    let north = enu.north_rate.unwrap_or(0.0);
    let up = enu.up_rate.unwrap_or(0.0);

    let speed_mps = (east * east + north * north).sqrt() as f32;
    // atan2(east, north) gives bearing measured clockwise from North
    let bearing_deg = east.atan2(north).to_degrees() as f32;
    let bearing = if bearing_deg < 0.0 {
        bearing_deg + 360.0
    } else {
        bearing_deg
    };

    Velocity {
        bearing,
        speed_mps,
        vertical_speed_mps: up as f32,
    }
}

// ── Main transform ────────────────────────────────────────────────────────────

/// Convert a SAPIENT `DetectionReport` to a peat-schema `Track`.
///
/// `sensor_position` must be provided when `location_oneof` is `RangeBearing`.
/// When it is `None` and the detection uses range-bearing coordinates, returns
/// `Err(SapientError::UnsupportedCoordinateSystem)`.  The caller (bridge layer)
/// should hold such detections in a pending queue until the sensor position
/// arrives via a `StatusReport`.
pub fn from_detection_report(
    node_id: &str,
    sensor_position: Option<&TrackPosition>,
    msg: &DetectionReport,
) -> Result<Track, SapientError> {
    let (position, pos_error) = match &msg.location_oneof {
        Some(LocationOneof::Location(loc)) => location_to_track_position(loc)?,
        Some(LocationOneof::RangeBearing(rb)) => {
            let sensor = sensor_position.ok_or_else(|| {
                SapientError::UnsupportedCoordinateSystem(
                    "range-bearing requires sensor position".into(),
                )
            })?;
            range_bearing_to_track_position(rb, sensor)?
        }
        None => {
            return Err(SapientError::MappingError {
                kind: "detection",
                detail: "DetectionReport has no location".into(),
            })
        }
    };

    let (classification, confidence) = msg
        .classification
        .first()
        .map(|c| {
            (
                c.r#type.clone().unwrap_or_default(),
                c.confidence.unwrap_or(0.0),
            )
        })
        .unwrap_or_default();

    let velocity = msg
        .velocity_oneof
        .as_ref()
        .map(|VelocityOneof::EnuVelocity(enu)| enu_to_velocity(enu));

    // Collect fields with no direct peat equivalent into attributes_json.
    let mut attrs = serde_json::Map::new();
    if let Some(colour) = &msg.colour {
        attrs.insert("colour".into(), serde_json::Value::String(colour.clone()));
    }
    if let Some(id) = &msg.id {
        attrs.insert("id".into(), serde_json::Value::String(id.clone()));
    }
    if !msg.behaviour.is_empty() {
        attrs.insert(
            "behaviours".into(),
            serde_json::Value::Array(
                msg.behaviour
                    .iter()
                    .filter_map(|b| {
                        b.r#type
                            .as_ref()
                            .map(|t| serde_json::Value::String(t.clone()))
                    })
                    .collect(),
            ),
        );
    }

    #[allow(deprecated)]
    Ok(Track {
        track_id: msg.object_id.clone().unwrap_or_default(),
        classification,
        confidence,
        position: Some(position),
        velocity,
        kinematics: None,
        position_error: Some(pos_error),
        state: TrackState::Confirmed as i32,
        source: Some(TrackSource {
            node_id: node_id.to_string(),
            sensor_id: msg.report_id.clone().unwrap_or_default(),
            model_version: String::new(),
            source_type: SourceType::Sensor as i32,
        }),
        attributes_json: serde_json::to_string(&attrs).unwrap_or_default(),
        first_seen: None,
        last_seen: None,
        observation_count: 1,
    })
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
        detection_report::{DetectionReportClassification, LocationOneof, VelocityOneof},
        DetectionReport, EnuVelocity, Location, LocationCoordinateSystem, LocationDatum,
        RangeBearing, RangeBearingCoordinateSystem, RangeBearingDatum,
    };

    fn wgs84_deg_location(lat: f64, lon: f64, alt: f64) -> Location {
        Location {
            x: Some(lon),
            y: Some(lat),
            z: Some(alt),
            coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
            datum: Some(LocationDatum::Wgs84E as i32),
            ..Default::default()
        }
    }

    fn simple_detection(lat: f64, lon: f64) -> DetectionReport {
        DetectionReport {
            report_id: Some("01HZZZZZZZZZZZZZZZZZZZZZZZZ".into()),
            object_id: Some("01HYYYYYYYYYYYYYYYYYYYYYYYY".into()),
            location_oneof: Some(LocationOneof::Location(wgs84_deg_location(lat, lon, 50.0))),
            ..Default::default()
        }
    }

    // ── Coordinate conversion ────────────────────────────────────────────────

    #[test]
    fn wgs84_deg_passthrough() {
        let track =
            from_detection_report("node-1", None, &simple_detection(51.5074, -0.1278)).unwrap();
        let pos = track.position.unwrap();
        assert!((pos.latitude - 51.5074).abs() < 1e-9);
        assert!((pos.longitude - (-0.1278)).abs() < 1e-9);
        assert!((pos.altitude - 50.0).abs() < 1e-3);
    }

    #[test]
    fn lat_lng_radians_converted_to_degrees() {
        let lat_deg = 51.5074_f64;
        let lon_deg = -0.1278_f64;
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(lon_deg.to_radians()),
                y: Some(lat_deg.to_radians()),
                z: Some(0.0),
                coordinate_system: Some(LocationCoordinateSystem::LatLngRadM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let track = from_detection_report("node-1", None, &report).unwrap();
        let pos = track.position.unwrap();
        assert!(
            (pos.latitude - lat_deg).abs() < 1e-4,
            "lat {}",
            pos.latitude
        );
        assert!(
            (pos.longitude - lon_deg).abs() < 1e-4,
            "lon {}",
            pos.longitude
        );
    }

    #[test]
    fn lat_lng_deg_feet_altitude_converted_to_metres() {
        // Raw coordinate_system value 3: degrees lat/lon, altitude in feet.
        // 100 ft = 30.48 m exactly.
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(-0.1278),
                y: Some(51.5074),
                z: Some(100.0), // feet
                coordinate_system: Some(3),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let track = from_detection_report("node-1", None, &report).unwrap();
        let pos = track.position.unwrap();
        assert!((pos.latitude - 51.5074).abs() < 1e-9);
        assert!((pos.longitude - (-0.1278)).abs() < 1e-9);
        assert!(
            (pos.altitude - 30.48).abs() < 1e-3,
            "100 ft should convert to 30.48 m, got {}",
            pos.altitude
        );
    }

    #[test]
    fn lat_lng_rad_feet_altitude_converted() {
        // Raw coordinate_system value 4: radians lat/lon, altitude in feet.
        let lat_deg = 51.5074_f64;
        let lon_deg = -0.1278_f64;
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(lon_deg.to_radians()),
                y: Some(lat_deg.to_radians()),
                z: Some(200.0), // feet → 60.96 m
                coordinate_system: Some(4),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let track = from_detection_report("node-1", None, &report).unwrap();
        let pos = track.position.unwrap();
        assert!(
            (pos.latitude - lat_deg).abs() < 1e-4,
            "lat {}",
            pos.latitude
        );
        assert!(
            (pos.longitude - lon_deg).abs() < 1e-4,
            "lon {}",
            pos.longitude
        );
        assert!(
            (pos.altitude - 60.96).abs() < 1e-2,
            "200 ft should convert to 60.96 m, got {}",
            pos.altitude
        );
    }

    #[test]
    fn utm_to_wgs84_central_london() {
        // Trafalgar Square: 51.5080°N, 0.1281°W → UTM 30U E=699651, N=5710164
        // (well-known reference used to validate the TM inverse projection)
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(699_651.0),
                y: Some(5_710_164.0),
                z: Some(11.0),
                coordinate_system: Some(LocationCoordinateSystem::UtmM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                utm_zone: Some("30U".into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let track = from_detection_report("node-1", None, &report).unwrap();
        let pos = track.position.unwrap();
        // 0.01° ≈ 1.1 km — P0 correctness check; TM inverse series converges to <1m for E<3°
        assert!(
            (pos.latitude - 51.508).abs() < 0.01,
            "latitude {} expected ~51.508",
            pos.latitude
        );
        assert!(
            (pos.longitude - (-0.1281)).abs() < 0.01,
            "longitude {} expected ~-0.1281",
            pos.longitude
        );
    }

    #[test]
    fn range_bearing_without_sensor_position_is_error() {
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::RangeBearing(RangeBearing {
                range: Some(100.0),
                azimuth: Some(45.0),
                elevation: Some(0.0),
                coordinate_system: Some(RangeBearingCoordinateSystem::DegreesM as i32),
                datum: Some(RangeBearingDatum::True as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let result = from_detection_report("node-1", None, &report);
        assert!(
            matches!(result, Err(SapientError::UnsupportedCoordinateSystem(_))),
            "expected UnsupportedCoordinateSystem, got {result:?}"
        );
    }

    #[test]
    fn range_bearing_with_sensor_position_resolves() {
        let sensor = TrackPosition {
            latitude: 51.5,
            longitude: -0.1,
            altitude: 0.0,
            cep_m: 0.0,
            vertical_error_m: 0.0,
        };
        // 100 m due North (azimuth=0)
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::RangeBearing(RangeBearing {
                range: Some(100.0),
                azimuth: Some(0.0),
                elevation: Some(0.0),
                coordinate_system: Some(RangeBearingCoordinateSystem::DegreesM as i32),
                datum: Some(RangeBearingDatum::True as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let track = from_detection_report("node-1", Some(&sensor), &report).unwrap();
        let pos = track.position.unwrap();
        // 100 m north ≈ 0.0009° latitude
        assert!(pos.latitude > 51.5, "target should be north of sensor");
        assert!(
            (pos.longitude - (-0.1)).abs() < 0.0001,
            "longitude unchanged"
        );
    }

    fn sensor() -> TrackPosition {
        TrackPosition {
            latitude: 51.5,
            longitude: -0.1,
            altitude: 0.0,
            cep_m: 0.0,
            vertical_error_m: 0.0,
        }
    }

    fn rb_report(range: f64, azimuth: f64, cs: i32) -> DetectionReport {
        DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::RangeBearing(RangeBearing {
                range: Some(range),
                azimuth: Some(azimuth),
                elevation: Some(0.0),
                coordinate_system: Some(cs),
                datum: Some(RangeBearingDatum::True as i32),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn range_bearing_radians_m_same_result_as_degrees_m() {
        let deg = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(100.0, 45.0, RangeBearingCoordinateSystem::DegreesM as i32),
        )
        .unwrap();
        let rad = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(
                100.0,
                45_f64.to_radians(),
                RangeBearingCoordinateSystem::RadiansM as i32,
            ),
        )
        .unwrap();
        let pd = deg.position.unwrap();
        let pr = rad.position.unwrap();
        assert!(
            (pd.latitude - pr.latitude).abs() < 1e-9,
            "lat mismatch: {} vs {}",
            pd.latitude,
            pr.latitude
        );
        assert!(
            (pd.longitude - pr.longitude).abs() < 1e-9,
            "lon mismatch: {} vs {}",
            pd.longitude,
            pr.longitude
        );
    }

    #[test]
    fn range_bearing_degrees_km_same_result_as_degrees_m() {
        let m = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(1000.0, 90.0, RangeBearingCoordinateSystem::DegreesM as i32),
        )
        .unwrap();
        let km = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(1.0, 90.0, RangeBearingCoordinateSystem::DegreesKm as i32),
        )
        .unwrap();
        let pm = m.position.unwrap();
        let pk = km.position.unwrap();
        assert!(
            (pm.latitude - pk.latitude).abs() < 1e-9,
            "lat: {} vs {}",
            pm.latitude,
            pk.latitude
        );
        assert!(
            (pm.longitude - pk.longitude).abs() < 1e-9,
            "lon: {} vs {}",
            pm.longitude,
            pk.longitude
        );
    }

    #[test]
    fn range_bearing_radians_km_same_result_as_degrees_m() {
        let m = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(500.0, 180.0, RangeBearingCoordinateSystem::DegreesM as i32),
        )
        .unwrap();
        let rkm = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(
                0.5,
                180_f64.to_radians(),
                RangeBearingCoordinateSystem::RadiansKm as i32,
            ),
        )
        .unwrap();
        let pm = m.position.unwrap();
        let pk = rkm.position.unwrap();
        assert!(
            (pm.latitude - pk.latitude).abs() < 1e-9,
            "lat: {} vs {}",
            pm.latitude,
            pk.latitude
        );
        assert!(
            (pm.longitude - pk.longitude).abs() < 1e-9,
            "lon: {} vs {}",
            pm.longitude,
            pk.longitude
        );
    }

    #[test]
    fn range_bearing_deprecated_degrees_feet_converted() {
        // Raw value 5: degrees/feet. 328.084 ft ≈ 100 m.
        let ft = from_detection_report("n", Some(&sensor()), &rb_report(328.084, 0.0, 5)).unwrap();
        let m = from_detection_report("n", Some(&sensor()), &rb_report(100.0, 0.0, 1)).unwrap();
        let pf = ft.position.unwrap();
        let pm = m.position.unwrap();
        assert!(
            (pf.latitude - pm.latitude).abs() < 1e-5,
            "lat: {} vs {}",
            pf.latitude,
            pm.latitude
        );
    }

    #[test]
    fn range_bearing_deprecated_radians_feet_converted() {
        // Raw value 6: radians/feet. 45° = π/4 rad; 100 m ≈ 328.084 ft.
        let rf = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(328.084, 45_f64.to_radians(), 6),
        )
        .unwrap();
        let dm = from_detection_report(
            "n",
            Some(&sensor()),
            &rb_report(100.0, 45.0, RangeBearingCoordinateSystem::DegreesM as i32),
        )
        .unwrap();
        let pr = rf.position.unwrap();
        let pd = dm.position.unwrap();
        assert!(
            (pr.latitude - pd.latitude).abs() < 1e-5,
            "lat: {} vs {}",
            pr.latitude,
            pd.latitude
        );
        assert!(
            (pr.longitude - pd.longitude).abs() < 1e-5,
            "lon: {} vs {}",
            pr.longitude,
            pd.longitude
        );
    }

    #[test]
    fn location_errors_produce_nonzero_cep_wgs84_deg() {
        // 0.001° error at equator ≈ 111 m; CEP ≈ 0.5887 × 2 × 111 ≈ 131 m
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(0.0),
                y: Some(0.0),
                z: Some(0.0),
                x_error: Some(0.001),
                y_error: Some(0.001),
                coordinate_system: Some(LocationCoordinateSystem::LatLngDegM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                ..Default::default()
            })),
            ..Default::default()
        };
        let pos = from_detection_report("n", None, &report)
            .unwrap()
            .position
            .unwrap();
        assert!(
            pos.cep_m > 100.0 && pos.cep_m < 200.0,
            "cep_m={}",
            pos.cep_m
        );
        assert_eq!(pos.vertical_error_m, 0.0);
    }

    #[test]
    fn location_errors_zero_when_not_provided() {
        let pos = from_detection_report("n", None, &simple_detection(51.0, 0.0))
            .unwrap()
            .position
            .unwrap();
        assert_eq!(pos.cep_m, 0.0);
        assert_eq!(pos.vertical_error_m, 0.0);
    }

    #[test]
    fn utm_errors_passed_through_as_cep() {
        // UTM errors are already in metres; 50 m each → CEP ≈ 0.5887 × 100 ≈ 58.9 m
        let report = DetectionReport {
            report_id: Some("r1".into()),
            object_id: Some("o1".into()),
            location_oneof: Some(LocationOneof::Location(Location {
                x: Some(699_651.0),
                y: Some(5_710_164.0),
                z: Some(0.0),
                x_error: Some(50.0),
                y_error: Some(50.0),
                coordinate_system: Some(LocationCoordinateSystem::UtmM as i32),
                datum: Some(LocationDatum::Wgs84E as i32),
                utm_zone: Some("30U".into()),
                ..Default::default()
            })),
            ..Default::default()
        };
        let pos = from_detection_report("n", None, &report)
            .unwrap()
            .position
            .unwrap();
        assert!(
            (pos.cep_m - 58.87).abs() < 1.0,
            "expected ~58.87 m CEP, got {}",
            pos.cep_m
        );
    }

    // ── Classification ───────────────────────────────────────────────────────

    #[test]
    fn classification_and_confidence_mapped() {
        let mut report = simple_detection(51.0, 0.0);
        report.classification = vec![DetectionReportClassification {
            r#type: Some("person".into()),
            confidence: Some(0.92),
            sub_class: vec![],
        }];
        let track = from_detection_report("node-1", None, &report).unwrap();
        assert_eq!(track.classification, "person");
        assert!((track.confidence - 0.92).abs() < 1e-5);
    }

    #[test]
    fn no_classification_defaults_to_empty() {
        let track = from_detection_report("node-1", None, &simple_detection(51.0, 0.0)).unwrap();
        assert_eq!(track.classification, "");
        assert_eq!(track.confidence, 0.0);
    }

    // ── Velocity ─────────────────────────────────────────────────────────────

    #[test]
    fn enu_velocity_due_north_maps_to_bearing_zero() {
        let mut report = simple_detection(51.0, 0.0);
        report.velocity_oneof = Some(VelocityOneof::EnuVelocity(EnuVelocity {
            east_rate: Some(0.0),
            north_rate: Some(5.0),
            up_rate: Some(0.0),
            ..Default::default()
        }));
        let track = from_detection_report("node-1", None, &report).unwrap();
        let vel = track.velocity.unwrap();
        assert!((vel.bearing - 0.0).abs() < 0.01, "bearing {}", vel.bearing);
        assert!((vel.speed_mps - 5.0).abs() < 0.01);
        assert!((vel.vertical_speed_mps - 0.0).abs() < 0.01);
    }

    #[test]
    fn enu_velocity_due_east_maps_to_bearing_90() {
        let mut report = simple_detection(51.0, 0.0);
        report.velocity_oneof = Some(VelocityOneof::EnuVelocity(EnuVelocity {
            east_rate: Some(3.0),
            north_rate: Some(0.0),
            up_rate: Some(1.5),
            ..Default::default()
        }));
        let track = from_detection_report("node-1", None, &report).unwrap();
        let vel = track.velocity.unwrap();
        assert!((vel.bearing - 90.0).abs() < 0.01, "bearing {}", vel.bearing);
        assert!((vel.speed_mps - 3.0).abs() < 0.01);
        assert!((vel.vertical_speed_mps - 1.5).abs() < 0.01);
    }

    #[test]
    fn no_velocity_yields_none() {
        let track = from_detection_report("node-1", None, &simple_detection(51.0, 0.0)).unwrap();
        assert!(track.velocity.is_none());
    }

    // ── Track identity fields ────────────────────────────────────────────────

    #[test]
    fn object_id_becomes_track_id() {
        let track = from_detection_report("node-1", None, &simple_detection(51.0, 0.0)).unwrap();
        assert_eq!(track.track_id, "01HYYYYYYYYYYYYYYYYYYYYYYYY");
    }

    #[test]
    fn source_node_id_preserved() {
        let track =
            from_detection_report("sensor-uuid-abc", None, &simple_detection(51.0, 0.0)).unwrap();
        assert_eq!(track.source.unwrap().node_id, "sensor-uuid-abc");
    }

    // ── Extension fields ─────────────────────────────────────────────────────

    #[test]
    fn colour_field_in_attributes_json() {
        let mut report = simple_detection(51.0, 0.0);
        report.colour = Some("red".into());
        let track = from_detection_report("node-1", None, &report).unwrap();
        let attrs: serde_json::Value = serde_json::from_str(&track.attributes_json).unwrap();
        assert_eq!(attrs["colour"], "red");
    }
}
