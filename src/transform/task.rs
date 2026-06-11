//! `peat_schema::command::v1::HierarchicalCommand` → outbound SAPIENT `Task`

use peat_schema::command::v1::{
    hierarchical_command::CommandType, mission_order::MissionType, HierarchicalCommand,
};
use ulid::Ulid;

use crate::{
    error::SapientError,
    proto::sapient_msg::bsi_flex_335_v2_0::{
        sapient_message::Content, task::Control, SapientMessage, Task,
    },
};

fn now_proto_ts() -> prost_types::Timestamp {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

fn command_to_task_name(cmd: &HierarchicalCommand) -> String {
    match &cmd.command_type {
        Some(CommandType::MissionOrder(mo)) => {
            let mt = MissionType::try_from(mo.mission_type).unwrap_or(MissionType::Unspecified);
            format!("MissionOrder/{mt:?}")
        }
        Some(CommandType::EngagementOrder(eo)) => {
            format!("EngagementOrder/{}", eo.target_id)
        }
        Some(CommandType::FormationChange(_)) => "FormationChange".into(),
        Some(CommandType::ConfigurationUpdate(_)) => "ConfigurationUpdate".into(),
        None => String::new(),
    }
}

/// Convert a peat-schema `HierarchicalCommand` to an outbound SAPIENT `SapientMessage`
/// wrapping a `Task`.
///
/// `source_node_id` — UUID of the SAPIENT node sending the task (the bridge/HLDMM).
/// `destination_node_id` — UUID of the DLMM that should execute the task.
///
/// Returns `Err(SapientError::TaskRejected)` when the command carries no command_type.
pub fn to_task(
    source_node_id: &str,
    destination_node_id: &str,
    cmd: &HierarchicalCommand,
) -> Result<SapientMessage, SapientError> {
    if cmd.command_type.is_none() {
        return Err(SapientError::TaskRejected {
            node_id: source_node_id.to_string(),
            reason: "HierarchicalCommand has no command_type".into(),
        });
    }

    let task_id = Ulid::new().to_string();
    let task_name = command_to_task_name(cmd);
    let now = now_proto_ts();

    let task = Task {
        task_id: Some(task_id),
        task_name: Some(task_name),
        task_description: Some(format!("peat command_id={}", cmd.command_id)),
        control: Some(Control::Start as i32),
        task_start_time: Some(now),
        task_end_time: cmd.expires_at.as_ref().map(|t| prost_types::Timestamp {
            seconds: t.seconds as i64,
            nanos: t.nanos as i32,
        }),
        region: vec![],
        command: None,
    };

    Ok(SapientMessage {
        timestamp: Some(now_proto_ts()),
        node_id: Some(source_node_id.to_string()),
        destination_id: Some(destination_node_id.to_string()),
        content: Some(Content::Task(task)),
        additional_information: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use peat_schema::command::v1::{
        hierarchical_command::CommandType, mission_order::MissionType, CommandTarget,
        EngagementOrder, HierarchicalCommand, MissionOrder,
    };

    fn isr_command() -> HierarchicalCommand {
        HierarchicalCommand {
            command_id: "cmd-001".into(),
            originator_id: "origin-node".into(),
            target: Some(CommandTarget {
                scope: 1, // INDIVIDUAL
                target_ids: vec!["target-node-uuid".into()],
            }),
            command_type: Some(CommandType::MissionOrder(MissionOrder {
                mission_type: MissionType::Isr as i32,
                mission_id: "mission-42".into(),
                description: "ISR sweep".into(),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    #[test]
    fn isr_mission_produces_start_task() {
        let msg = to_task("src-uuid", "dst-uuid", &isr_command()).unwrap();
        let task = match msg.content.unwrap() {
            Content::Task(t) => t,
            other => panic!("expected Task, got {other:?}"),
        };
        assert_eq!(task.control, Some(Control::Start as i32));
    }

    #[test]
    fn task_id_is_valid_ulid() {
        let msg = to_task("src-uuid", "dst-uuid", &isr_command()).unwrap();
        let task = match msg.content.unwrap() {
            Content::Task(t) => t,
            _ => panic!(),
        };
        let task_id = task.task_id.unwrap();
        assert_eq!(task_id.len(), 26, "ULID should be 26 chars, got {task_id}");
        assert!(
            task_id.chars().all(|c| c.is_ascii_alphanumeric()),
            "ULID should be alphanumeric, got {task_id}"
        );
    }

    #[test]
    fn source_node_id_on_outer_message() {
        let msg = to_task("source-node-uuid", "dst-uuid", &isr_command()).unwrap();
        assert_eq!(msg.node_id.as_deref(), Some("source-node-uuid"));
    }

    #[test]
    fn destination_node_id_on_outer_message() {
        let msg = to_task("src-uuid", "destination-node-uuid", &isr_command()).unwrap();
        assert_eq!(msg.destination_id.as_deref(), Some("destination-node-uuid"));
    }

    #[test]
    fn timestamp_is_set_and_nonzero() {
        let msg = to_task("src", "dst", &isr_command()).unwrap();
        let ts = msg.timestamp.unwrap();
        assert!(ts.seconds > 0, "timestamp.seconds should be > 0");
    }

    #[test]
    fn task_start_time_is_set() {
        let msg = to_task("src", "dst", &isr_command()).unwrap();
        let task = match msg.content.unwrap() {
            Content::Task(t) => t,
            _ => panic!(),
        };
        assert!(task.task_start_time.is_some());
    }

    #[test]
    fn no_command_type_returns_task_rejected() {
        let cmd = HierarchicalCommand {
            command_id: "empty".into(),
            ..Default::default()
        };
        let result = to_task("src", "dst", &cmd);
        assert!(
            matches!(result, Err(SapientError::TaskRejected { .. })),
            "expected TaskRejected, got {result:?}"
        );
    }

    #[test]
    fn task_name_includes_mission_type() {
        let msg = to_task("src", "dst", &isr_command()).unwrap();
        let task = match msg.content.unwrap() {
            Content::Task(t) => t,
            _ => panic!(),
        };
        let name = task.task_name.unwrap();
        assert!(
            name.contains("Isr"),
            "task name '{name}' should contain 'Isr'"
        );
    }

    #[test]
    fn engagement_order_produces_start_task() {
        let cmd = HierarchicalCommand {
            command_id: "eng-cmd".into(),
            command_type: Some(CommandType::EngagementOrder(EngagementOrder {
                target_id: "tgt-001".into(),
                engagement_type: 1,
                ..Default::default()
            })),
            ..Default::default()
        };
        let msg = to_task("src", "dst", &cmd).unwrap();
        let task = match msg.content.unwrap() {
            Content::Task(t) => t,
            _ => panic!(),
        };
        assert_eq!(task.control, Some(Control::Start as i32));
    }
}
