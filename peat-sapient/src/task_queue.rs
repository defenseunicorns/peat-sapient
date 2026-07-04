//! Per-node DIL outbound task queue.
//!
//! When the bridge sends a SAPIENT `Task` to a DLMM, the task is enqueued here
//! before (or simultaneous with) delivery. It remains queued until the DLMM
//! sends `TaskAck::Accepted`. If the DLMM disconnects before acknowledging, the
//! queue is replayed in insertion order on reconnect. Tasks older than their TTL
//! are expired with a warning rather than replayed — a stale command is worse than
//! no command when operational context has changed.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use tokio::time::Instant;
use tracing::warn;

use crate::proto::SapientMessage;

struct QueuedTask {
    msg: SapientMessage,
    task_id: String,
    enqueued_at: Instant,
    ttl: Duration,
}

/// Per-node FIFO queue for outbound SAPIENT `Task` messages.
///
/// # Invariants
///
/// - At most `max_depth` tasks per node. Enqueueing beyond capacity evicts the
///   oldest pending task (with a warning) to make room for the new one.
/// - `drain_expired` removes tasks whose TTL has elapsed and should be called
///   before `pending_for` on reconnect, and periodically in the bridge loop.
/// - `ack` removes a task when `TaskAck::Accepted` or `TaskAck::Rejected` is
///   received. Duplicate acks are safe (no-op).
pub struct TaskQueue {
    queues: HashMap<String, VecDeque<QueuedTask>>,
    max_depth: usize,
    default_ttl: Duration,
}

impl TaskQueue {
    pub fn new(max_depth: usize, default_ttl: Duration) -> Self {
        Self {
            queues: HashMap::new(),
            max_depth,
            default_ttl,
        }
    }

    /// Enqueue a `SapientMessage` carrying a `Task` for `node_id`.
    ///
    /// If the per-node queue is already at `max_depth`, the oldest pending task
    /// is evicted with a warning before the new task is added. Returns the
    /// `task_id` of the evicted task, if any, so callers can clean up
    /// associated state (e.g. command_id correlation).
    pub fn enqueue(
        &mut self,
        node_id: &str,
        task_id: String,
        msg: SapientMessage,
    ) -> Option<String> {
        let queue = self.queues.entry(node_id.to_string()).or_default();
        let evicted = if queue.len() >= self.max_depth {
            queue.pop_front().map(|dropped| {
                warn!(
                    node_id = %node_id,
                    dropped_task_id = %dropped.task_id,
                    "DIL task queue full — evicted oldest pending task"
                );
                dropped.task_id
            })
        } else {
            None
        };
        queue.push_back(QueuedTask {
            msg,
            task_id,
            enqueued_at: Instant::now(),
            ttl: self.default_ttl,
        });
        evicted
    }

    /// Acknowledge a task — remove it from the queue.
    ///
    /// Call when `TaskAck::Accepted` or `TaskAck::Rejected` arrives. No-op if
    /// `task_id` is not in the queue for `node_id` (duplicate acks are safe).
    pub fn ack(&mut self, node_id: &str, task_id: &str) {
        if let Some(queue) = self.queues.get_mut(node_id) {
            queue.retain(|t| t.task_id != task_id);
        }
    }

    /// Expire and discard tasks whose TTL has elapsed.
    ///
    /// Each expired task produces a `warn!` log entry. Returns the `task_id`s
    /// of all expired tasks so callers can clean up associated state (e.g.
    /// command_id correlation). Call this before `pending_for` on reconnect
    /// and periodically in the bridge loop.
    pub fn drain_expired(&mut self) -> Vec<String> {
        let now = Instant::now();
        let mut expired_ids = Vec::new();
        for (node_id, queue) in &mut self.queues {
            queue.retain(|t| {
                if now.duration_since(t.enqueued_at) > t.ttl {
                    warn!(
                        node_id = %node_id,
                        task_id = %t.task_id,
                        ttl = ?t.ttl,
                        "queued task TTL expired — discarding without replay"
                    );
                    expired_ids.push(t.task_id.clone());
                    false
                } else {
                    true
                }
            });
        }
        expired_ids
    }

    /// Return clones of all pending (non-expired) tasks for `node_id` in
    /// insertion order.
    ///
    /// Does **not** remove tasks from the queue — call `ack` to remove after
    /// the DLMM acknowledges each replayed task.
    pub fn pending_for(&self, node_id: &str) -> Vec<SapientMessage> {
        self.queues
            .get(node_id)
            .map(|q| q.iter().map(|t| t.msg.clone()).collect())
            .unwrap_or_default()
    }

    /// Returns `true` when `node_id` has no pending (queued) tasks.
    pub fn is_empty_for(&self, node_id: &str) -> bool {
        self.queues
            .get(node_id)
            .map(|q| q.is_empty())
            .unwrap_or(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn task_msg(task_id: &str) -> SapientMessage {
        use crate::proto::sapient_msg::bsi_flex_335_v2_0::{
            sapient_message::Content, task::Control, Task,
        };
        SapientMessage {
            node_id: Some("hldmm-1".into()),
            destination_id: Some("dlmm-1".into()),
            content: Some(Content::Task(Task {
                task_id: Some(task_id.to_string()),
                task_name: Some("test".into()),
                control: Some(Control::Start as i32),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn task_ids(msgs: &[SapientMessage]) -> Vec<String> {
        msgs.iter()
            .filter_map(|m| m.content.as_ref())
            .filter_map(|c| {
                if let crate::proto::sapient_msg::bsi_flex_335_v2_0::sapient_message::Content::Task(t) = c {
                    t.task_id.clone()
                } else {
                    None
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn enqueue_adds_task_to_pending() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        assert_eq!(q.pending_for("node-1").len(), 1);
    }

    #[tokio::test]
    async fn pending_for_unknown_node_returns_empty() {
        let q = TaskQueue::new(10, Duration::from_secs(60));
        assert!(q.pending_for("ghost").is_empty());
    }

    #[tokio::test]
    async fn pending_for_returns_insertion_order() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        q.enqueue("node-1", "task-b".into(), task_msg("task-b"));
        q.enqueue("node-1", "task-c".into(), task_msg("task-c"));
        assert_eq!(
            task_ids(&q.pending_for("node-1")),
            ["task-a", "task-b", "task-c"]
        );
    }

    #[tokio::test]
    async fn ack_removes_acknowledged_task() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        q.enqueue("node-1", "task-b".into(), task_msg("task-b"));
        q.ack("node-1", "task-a");
        assert_eq!(task_ids(&q.pending_for("node-1")), ["task-b"]);
    }

    #[tokio::test]
    async fn ack_unknown_task_is_noop() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        q.ack("node-1", "ghost-task");
        assert_eq!(q.pending_for("node-1").len(), 1);
    }

    #[tokio::test]
    async fn ack_on_unknown_node_is_noop() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.ack("ghost-node", "task-x"); // must not panic
    }

    #[tokio::test]
    async fn queue_full_evicts_oldest_task() {
        let mut q = TaskQueue::new(2, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        q.enqueue("node-1", "task-b".into(), task_msg("task-b"));
        // Third task exceeds max_depth=2 — task-a must be evicted.
        q.enqueue("node-1", "task-c".into(), task_msg("task-c"));
        let ids = task_ids(&q.pending_for("node-1"));
        assert_eq!(ids.len(), 2);
        assert!(
            !ids.contains(&"task-a".to_string()),
            "task-a should have been evicted"
        );
        assert!(ids.contains(&"task-b".to_string()));
        assert!(ids.contains(&"task-c".to_string()));
    }

    #[tokio::test]
    async fn is_empty_for_unknown_node_returns_true() {
        let q = TaskQueue::new(10, Duration::from_secs(60));
        assert!(q.is_empty_for("ghost"));
    }

    #[tokio::test]
    async fn is_empty_for_returns_false_when_tasks_present() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        assert!(!q.is_empty_for("node-1"));
    }

    #[tokio::test]
    async fn nodes_have_independent_queues() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-a".into(), task_msg("task-a"));
        q.enqueue("node-2", "task-b".into(), task_msg("task-b"));
        assert_eq!(q.pending_for("node-1").len(), 1);
        assert_eq!(q.pending_for("node-2").len(), 1);
        q.ack("node-1", "task-a");
        assert!(q.is_empty_for("node-1"));
        assert!(!q.is_empty_for("node-2"));
    }

    #[tokio::test(start_paused = true)]
    async fn drain_expired_removes_stale_tasks() {
        let mut q = TaskQueue::new(10, Duration::from_secs(30));
        q.enqueue("node-1", "task-old".into(), task_msg("task-old"));

        tokio::time::advance(Duration::from_secs(31)).await;
        q.drain_expired();

        assert!(q.is_empty_for("node-1"), "stale task should be expired");
    }

    #[tokio::test(start_paused = true)]
    async fn drain_expired_preserves_fresh_tasks() {
        let mut q = TaskQueue::new(10, Duration::from_secs(60));
        q.enqueue("node-1", "task-fresh".into(), task_msg("task-fresh"));

        tokio::time::advance(Duration::from_secs(30)).await;
        q.drain_expired();

        assert_eq!(
            q.pending_for("node-1").len(),
            1,
            "fresh task should survive"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn drain_expired_selective_within_node() {
        let ttl = Duration::from_secs(30);
        let mut q = TaskQueue::new(10, ttl);
        q.enqueue("node-1", "task-old".into(), task_msg("task-old"));

        // Advance past TTL, then add a fresh task.
        tokio::time::advance(Duration::from_secs(31)).await;
        q.enqueue("node-1", "task-fresh".into(), task_msg("task-fresh"));
        q.drain_expired();

        let ids = task_ids(&q.pending_for("node-1"));
        assert_eq!(ids, ["task-fresh"], "only the fresh task should remain");
    }
}
