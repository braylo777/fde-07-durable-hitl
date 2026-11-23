//! Stage 7 — Human-in-the-Loop (HITL) & Durable Workflows.
//!
//! * `engine` — an event-sourced workflow engine (the Temporal model, in miniature): the
//!   history is the checkpoint, state is a pure fold over it, and a crashed run resumes by
//!   replaying history without re-running finished steps. Tasks retry with backoff;
//!   failures trigger compensations (saga); approval gates wait for human votes with quorum,
//!   role checks, separation of duties and timeouts with escalation.
//! * `notify` — Slack Block Kit and Microsoft Teams Adaptive Card approval messages, and
//!   Slack request-signature verification (HMAC-SHA256, 5-minute replay window).
//! * `trail` — a hash-chained accountability trail of every human decision.

mod api;
pub mod engine;
pub mod notify;
pub mod trail;

pub use api::dispatch;
use serde_json::Value;

pub const OPS: &[&str] = &["run", "crash_test", "approval_message", "verify_slack_signature"];

pub(crate) fn str_field<'a>(v: &'a Value, k: &str) -> Result<&'a str, String> {
    v.get(k).and_then(Value::as_str).ok_or_else(|| format!("missing string field `{k}`"))
}

pub fn handle(op: &str, i: &Value) -> Result<Value, String> {
    match op {
        "run" => engine::run_op(i),
        "crash_test" => engine::crash_test_op(i),
        "approval_message" => notify::message_op(i),
        "verify_slack_signature" => notify::verify_op(i),
        _ => Err(format!("unknown op `{op}`; known: {}", OPS.join(", "))),
    }
}
