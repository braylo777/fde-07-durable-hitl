//! Accountability trail: every human decision and every side effect, hash-chained so the
//! record can be shown to legal or an auditor and any later edit is detectable.

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn from_history(h: &[Value]) -> Vec<Value> {
    let mut prev = "0".repeat(64);
    let mut out = Vec::new();
    for e in h {
        let t = e["type"].as_str().unwrap_or("");
        let entry = match t {
            "ApprovalVote" => {
                json!({"who": e["user"], "role": e["role"], "did": e["decision"], "step": e["step"], "reason": e["reason"], "at_ms": e["t"]})
            }
            "SignalRejected" => {
                json!({"who": e["user"], "did": "vote refused", "reason": e["reason"], "at_ms": e["t"]})
            }
            "Escalated" => {
                json!({"who": "system", "did": "escalated", "to": e["to"], "step": e["step"], "at_ms": e["t"]})
            }
            "StepCompleted" if e["idempotency_key"].is_string() => {
                json!({"who": "system", "did": format!("ran {}", e["step"].as_str().unwrap_or("")), "key": e["idempotency_key"], "at_ms": e["t"]})
            }
            "Compensated" => {
                json!({"who": "system", "did": format!("compensated {}", e["step"].as_str().unwrap_or("")), "at_ms": e["t"]})
            }
            "WorkflowRejected" | "WorkflowCompleted" | "WorkflowFailed" => {
                json!({"who": "system", "did": t, "at_ms": e["t"]})
            }
            _ => continue,
        };
        let mut hasher = Sha256::new();
        hasher.update(prev.as_bytes());
        hasher.update(entry.to_string().as_bytes());
        let hash: String = hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let mut rec = entry;
        rec["hash"] = json!(hash.clone());
        out.push(rec);
        prev = hash;
    }
    out
}
