//! Event-sourced durable workflow engine with human approval gates.

use serde::Deserialize;
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Deserialize)]
pub struct Step {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String, // "task" | "approval"
    #[serde(default)]
    pub task: String,
    #[serde(default = "three")]
    pub max_attempts: u32,
    #[serde(default)]
    pub compensate: Option<String>,
    // approval fields
    #[serde(default)]
    pub approver_roles: Vec<String>,
    #[serde(default = "one")]
    pub quorum: usize,
    #[serde(default = "day")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub escalate_to: Vec<String>,
    /// Only require this gate when ctx[when_field] > when_gt (e.g. refunds over 500).
    #[serde(default)]
    pub when_field: Option<String>,
    #[serde(default)]
    pub when_gt: Option<f64>,
}
fn three() -> u32 {
    3
}
fn one() -> usize {
    1
}
fn day() -> u64 {
    86_400_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct Def {
    pub id: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Signal {
    pub id: String,
    pub at_ms: u64,
    pub user: String,
    pub role: String,
    pub decision: String, // "approve" | "deny"
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Default, Clone)]
struct State {
    idx: usize,
    ctx: Map<String, Value>,
    status: String,
    now: u64,
    attempts: u32,
    votes: Vec<(String, String)>, // (user, decision)
    deadline: Option<u64>,
    escalated: bool,
    consumed: Vec<String>,
    completed: Vec<usize>,
}

/// Deterministic task registry. Side effects go to `effects` keyed by idempotency key,
/// so a task re-run after a crash finds its earlier effect instead of repeating it.
fn run_task(
    name: &str,
    ctx: &Map<String, Value>,
    key: &str,
    effects: &mut Vec<Value>,
) -> Result<Value, String> {
    if let Some(prev) = effects.iter().find(|e| e["key"] == key) {
        return Ok(prev["output"].clone()); // idempotent replay of an external side effect
    }
    let out = match name {
        "lookup_customer" => {
            let id = ctx
                .get("customer_id")
                .and_then(Value::as_str)
                .ok_or("ctx.customer_id missing")?;
            let tier = if id.ends_with('1') {
                "gold"
            } else {
                "standard"
            };
            json!({"tier": tier, "lifetime_value": if tier == "gold" { 48200 } else { 3100 }})
        }
        "draft_refund" => {
            let amt = ctx
                .get("amount")
                .and_then(Value::as_f64)
                .ok_or("ctx.amount missing")?;
            json!({"refund_amount": amt, "draft": format!("Refund of ${amt:.2} for order {}", ctx.get("order_id").and_then(Value::as_str).unwrap_or("?"))})
        }
        "issue_refund" => {
            json!({"refund_id": format!("rf_{}", &key.replace(':', "_")), "issued": true})
        }
        "reverse_refund" => json!({"reversed": true}),
        "notify_customer" => json!({"notified": true}),
        "close_ticket" => json!({"ticket_closed": true}),
        other => return Err(format!("unknown task {other}")),
    };
    let side_effect = matches!(name, "issue_refund" | "reverse_refund" | "notify_customer");
    if side_effect {
        effects.push(json!({"key": key, "task": name, "output": out}));
    }
    Ok(out)
}

fn fold(history: &[Value]) -> State {
    let mut s = State {
        status: "running".into(),
        ..Default::default()
    };
    for e in history {
        s.now = s.now.max(e["t"].as_u64().unwrap_or(0));
        match e["type"].as_str().unwrap_or("") {
            "WorkflowStarted" => s.ctx = e["input"].as_object().cloned().unwrap_or_default(),
            "StepStarted" => s.attempts = e["attempt"].as_u64().unwrap_or(1) as u32,
            "StepCompleted" => {
                if let Some(o) = e["output"].as_object() {
                    s.ctx.extend(o.clone());
                }
                s.completed.push(s.idx);
                s.idx += 1;
                s.attempts = 0;
                s.votes.clear();
                s.deadline = None;
                s.escalated = false;
            }
            "StepSkipped" => {
                s.idx += 1;
            }
            "ApprovalRequested" => s.deadline = e["deadline"].as_u64(),
            "ApprovalVote" => {
                s.votes.push((
                    e["user"].as_str().unwrap_or("").into(),
                    e["decision"].as_str().unwrap_or("").into(),
                ));
                s.consumed.push(e["signal"].as_str().unwrap_or("").into());
            }
            "SignalRejected" => s.consumed.push(e["signal"].as_str().unwrap_or("").into()),
            "Escalated" => {
                s.escalated = true;
                s.deadline = e["deadline"].as_u64();
            }
            "WorkflowCompleted" => s.status = "completed".into(),
            "WorkflowFailed" => s.status = "failed".into(),
            "WorkflowRejected" => s.status = "rejected".into(),
            _ => {}
        }
    }
    s
}

/// Idempotency keys are per workflow *run*, not per definition: `input.run_id`, or a hash of
/// the start input, so two refunds for two different orders never share a key.
fn run_id(def: &Def, history: &[Value]) -> String {
    let input = history.first().map(|e| e["input"].clone()).unwrap_or(Value::Null);
    match input["run_id"].as_str() {
        Some(r) => format!("{}/{r}", def.id),
        None => {
            use sha2::{Digest, Sha256};
            let h = Sha256::digest(input.to_string().as_bytes());
            format!("{}/{}", def.id, h.iter().take(6).map(|b| format!("{b:02x}")).collect::<String>())
        }
    }
}

pub struct Run {
    pub history: Vec<Value>,
    pub effects: Vec<Value>,
    pub status: String,
}

/// Continue a workflow from `history`. Stops when it completes, fails, needs a signal that has
/// not arrived yet ("waiting"), or the history reaches `crash_at` events ("crashed").
pub fn advance(
    def: &Def,
    input: &Value,
    mut history: Vec<Value>,
    signals: &[Signal],
    fail_times: &Map<String, Value>,
    mut effects: Vec<Value>,
    crash_at: Option<usize>,
) -> Run {
    macro_rules! emit {
        ($s:expr, $e:expr) => {{
            let mut ev = $e;
            ev["seq"] = json!(history.len());
            ev["t"] = json!($s.now);
            history.push(ev);
            if crash_at == Some(history.len()) {
                return Run {
                    history,
                    effects,
                    status: "crashed".into(),
                };
            }
        }};
    }
    if history.is_empty() {
        let s = State::default();
        emit!(
            s,
            json!({"type": "WorkflowStarted", "workflow": def.id, "input": input})
        );
    }
    let mut sigs: Vec<&Signal> = signals.iter().collect();
    sigs.sort_by_key(|x| x.at_ms);
    loop {
        let mut s = fold(&history);
        if s.status != "running" {
            return Run {
                history,
                effects,
                status: s.status,
            };
        }
        let Some(step) = def.steps.get(s.idx) else {
            emit!(s, json!({"type": "WorkflowCompleted", "result": s.ctx}));
            continue;
        };
        match step.kind.as_str() {
            "task" => {
                // One event per loop pass, decided only from history: a crash between any two
                // events resumes to exactly the same history.
                let last = history.last().cloned().unwrap_or(json!({}));
                let same = last["step"] == step.name;
                let failed_final = history.iter().any(|e| e["type"] == "StepFailed" && e["step"] == step.name && e["retry"] == false);
                if failed_final {
                    let done: Vec<&str> = history.iter().filter(|e| e["type"] == "Compensated").filter_map(|e| e["step"].as_str()).collect();
                    let next = s.completed.iter().rev().copied().find(|&d| def.steps[d].compensate.is_some() && !done.contains(&def.steps[d].name.as_str()));
                    match next {
                        Some(d) => {
                            let c = def.steps[d].compensate.clone().unwrap();
                            let out = run_task(&c, &s.ctx, &format!("{}:{}:compensate", run_id(def, &history), def.steps[d].name), &mut effects).unwrap_or(json!({}));
                            emit!(s, json!({"type": "Compensated", "step": def.steps[d].name, "with": c, "output": out}));
                        }
                        None => emit!(s, json!({"type": "WorkflowFailed", "step": step.name})),
                    }
                    continue;
                }
                if same && last["type"] == "StepFailed" {
                    let a = last["attempt"].as_u64().unwrap_or(1) as u32;
                    s.now += 1000u64.saturating_mul(1u64 << (a.max(1) - 1).min(20)); // 1s, 2s, 4s… capped
                    emit!(s, json!({"type": "TimerFired", "step": step.name, "reason": "retry backoff"}));
                    continue;
                }
                if !(same && last["type"] == "StepStarted") {
                    emit!(s, json!({"type": "StepStarted", "step": step.name, "attempt": s.attempts + 1}));
                    continue;
                }
                // The attempt in `last` is in flight (or was interrupted by a crash): run it.
                let attempt = s.attempts;
                let injected = fail_times.get(&step.task).and_then(Value::as_u64).unwrap_or(0) as u32;
                let key = format!("{}:{}", run_id(def, &history), step.name);
                let res = if attempt <= injected { Err("upstream 503 (injected)".to_string()) } else { run_task(&step.task, &s.ctx, &key, &mut effects) };
                match res {
                    Ok(out) => emit!(s, json!({"type": "StepCompleted", "step": step.name, "output": out, "idempotency_key": key})),
                    Err(e) => emit!(s, json!({"type": "StepFailed", "step": step.name, "attempt": attempt, "error": e, "retry": attempt < step.max_attempts})),
                }
            }
            "approval" => {
                if let (Some(f), Some(gt)) = (&step.when_field, step.when_gt) {
                    let v = s.ctx.get(f).and_then(Value::as_f64).unwrap_or(0.0);
                    if v <= gt {
                        emit!(s, json!({"type": "StepSkipped", "step": step.name, "reason": format!("{f} = {v} is not over {gt}")}));
                        continue;
                    }
                }
                let Some(deadline) = s.deadline else {
                    emit!(s, json!({"type": "ApprovalRequested", "step": step.name, "roles": step.approver_roles, "quorum": step.quorum, "deadline": s.now + step.timeout_ms}));
                    continue;
                };
                // Decide from recorded votes first (also covers a crash right after a vote).
                let last = history.last().cloned().unwrap_or(json!({}));
                if last["type"] == "TimerFired" && last["step"] == step.name {
                    emit!(s, json!({"type": "WorkflowRejected", "step": step.name, "reason": "no decision before deadline"}));
                    continue;
                }
                if let Some(d) = history.iter().find(|e| e["type"] == "ApprovalVote" && e["step"] == step.name && e["decision"] == "deny") {
                    emit!(s, json!({"type": "WorkflowRejected", "step": step.name, "by": d["user"], "reason": d["reason"]}));
                    continue;
                }
                if s.votes.iter().filter(|(_, d)| d == "approve").count() >= step.quorum {
                    let who: Vec<String> = s.votes.iter().map(|(u, _)| u.clone()).collect();
                    emit!(s, json!({"type": "StepCompleted", "step": step.name, "output": {format!("{}_approved_by", step.name): who}}));
                    continue;
                }
                let Some(sig) = sigs.iter().find(|x| !s.consumed.contains(&x.id)) else {
                    return Run { history, effects, status: "waiting".into() };
                };
                if sig.at_ms > deadline {
                    s.now = deadline;
                    if !s.escalated && !step.escalate_to.is_empty() {
                        emit!(s, json!({"type": "Escalated", "step": step.name, "to": step.escalate_to, "deadline": deadline + step.timeout_ms}));
                    } else {
                        emit!(s, json!({"type": "TimerFired", "step": step.name, "reason": "approval timeout"}));
                    }
                    continue;
                }
                s.now = s.now.max(sig.at_ms);
                let roles: Vec<&String> = if s.escalated { step.approver_roles.iter().chain(step.escalate_to.iter()).collect() } else { step.approver_roles.iter().collect() };
                let requester = s.ctx.get("requested_by").and_then(Value::as_str).unwrap_or("");
                let why_bad = if !roles.contains(&&sig.role) {
                    Some(format!("role {} cannot approve this step", sig.role))
                } else if sig.user == requester {
                    Some("separation of duties: requester cannot approve own request".to_string())
                } else if s.votes.iter().any(|(u, _)| u == &sig.user) {
                    Some("duplicate vote".to_string())
                } else if !matches!(sig.decision.as_str(), "approve" | "deny") {
                    Some("decision must be approve or deny".to_string())
                } else {
                    None
                };
                match why_bad {
                    Some(r) => emit!(s, json!({"type": "SignalRejected", "signal": sig.id, "user": sig.user, "reason": r})),
                    None => emit!(s, json!({"type": "ApprovalVote", "step": step.name, "signal": sig.id, "user": sig.user, "role": sig.role, "decision": sig.decision, "reason": sig.reason})),
                }
            }
            other => {
                emit!(
                    s,
                    json!({"type": "WorkflowFailed", "step": step.name, "error": format!("unknown step type {other}")})
                );
            }
        }
    }
}

fn parse_inputs(i: &Value) -> Result<(Def, Value, Vec<Signal>, Map<String, Value>), String> {
    let def: Def = serde_json::from_value(i.get("workflow").cloned().ok_or("missing `workflow`")?)
        .map_err(|e| format!("workflow: {e}"))?;
    let signals: Vec<Signal> =
        serde_json::from_value(i.get("signals").cloned().unwrap_or(json!([])))
            .map_err(|e| format!("signals: {e}"))?;
    let fails = i["fail_times"].as_object().cloned().unwrap_or_default();
    Ok((
        def,
        i.get("input").cloned().unwrap_or(json!({})),
        signals,
        fails,
    ))
}

pub fn run_op(i: &Value) -> Result<Value, String> {
    let (def, input, signals, fails) = parse_inputs(i)?;
    let history: Vec<Value> = i["history"].as_array().cloned().unwrap_or_default();
    let r = advance(&def, &input, history, &signals, &fails, vec![], None);
    let trail = crate::trail::from_history(&r.history);
    let st = fold(&r.history);
    Ok(
        json!({"status": r.status, "context": st.ctx, "history": r.history, "side_effects": r.effects, "accountability_trail": trail}),
    )
}

/// Run uninterrupted, then crash after N events and resume from the saved history.
/// Passes if both end in the same state and every side effect happened exactly once.
pub fn crash_test_op(i: &Value) -> Result<Value, String> {
    let (def, input, signals, fails) = parse_inputs(i)?;
    let n = i["crash_after_events"]
        .as_u64()
        .ok_or("missing `crash_after_events`")? as usize;
    let clean = advance(&def, &input, vec![], &signals, &fails, vec![], None);
    let crashed = advance(&def, &input, vec![], &signals, &fails, vec![], Some(n));
    let saved = crashed.history.clone();
    let resumed = advance(
        &def,
        &input,
        saved.clone(),
        &signals,
        &fails,
        crashed.effects.clone(),
        None,
    );
    let strip = |h: &[Value]| {
        h.iter()
            .map(|e| {
                e["type"].as_str().unwrap_or("").to_string()
                    + ":"
                    + e["step"].as_str().unwrap_or("")
            })
            .collect::<Vec<_>>()
    };
    let mut keys: Vec<String> = resumed
        .effects
        .iter()
        .filter_map(|e| e["key"].as_str().map(String::from))
        .collect();
    let total = keys.len();
    keys.sort();
    keys.dedup();
    let same = strip(&clean.history) == strip(&resumed.history) && clean.status == resumed.status;
    Ok(json!({
        "pass": same && keys.len() == total,
        "uninterrupted_status": clean.status,
        "crashed_at_event": saved.len(), "crashed_last_event": saved.last().map(|e| e["type"].clone()),
        "resumed_status": resumed.status,
        "histories_match": same,
        "side_effects_exactly_once": keys.len() == total,
        "side_effects": resumed.effects
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn wf() -> Value {
        json!({"id": "refund-42", "steps": [
            {"name": "lookup", "type": "task", "task": "lookup_customer"},
            {"name": "draft", "type": "task", "task": "draft_refund"},
            {"name": "manager_ok", "type": "approval", "approver_roles": ["support_manager"], "quorum": 1, "timeout_ms": 3600000,
             "escalate_to": ["director"], "when_field": "refund_amount", "when_gt": 500},
            {"name": "issue", "type": "task", "task": "issue_refund", "compensate": "reverse_refund"},
            {"name": "notify", "type": "task", "task": "notify_customer"}]})
    }
    fn input(amount: f64) -> Value {
        json!({"customer_id": "C-1001", "order_id": "SO-77", "amount": amount, "requested_by": "agent-7"})
    }
    fn sig(id: &str, at: u64, user: &str, role: &str, d: &str) -> Value {
        json!({"id": id, "at_ms": at, "user": user, "role": role, "decision": d})
    }
    fn run(i: Value) -> Value {
        run_op(&i).unwrap()
    }

    #[test]
    fn small_refund_skips_gate() {
        let r = run(json!({"workflow": wf(), "input": input(80.0)}));
        assert_eq!(r["status"], "completed");
        assert!(r["history"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "StepSkipped"));
    }
    #[test]
    fn large_refund_waits_then_approves() {
        let r = run(json!({"workflow": wf(), "input": input(900.0)}));
        assert_eq!(r["status"], "waiting");
        let r = run(
            json!({"workflow": wf(), "input": input(900.0), "signals": [sig("s1", 60000, "dana", "support_manager", "approve")]}),
        );
        assert_eq!(r["status"], "completed");
        assert_eq!(r["context"]["manager_ok_approved_by"], json!(["dana"]));
    }
    #[test]
    fn wrong_role_and_self_approval_rejected() {
        let mut i = input(900.0);
        i["requested_by"] = json!("dana");
        let r = run(json!({"workflow": wf(), "input": i, "signals": [
            sig("s1", 1000, "eve", "intern", "approve"), sig("s2", 2000, "dana", "support_manager", "approve")]}));
        assert_eq!(r["status"], "waiting");
        let rej: Vec<_> = r["history"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["type"] == "SignalRejected")
            .collect();
        assert_eq!(rej.len(), 2);
    }
    #[test]
    fn deny_rejects() {
        let r = run(
            json!({"workflow": wf(), "input": input(900.0), "signals": [sig("s1", 1000, "dana", "support_manager", "deny")]}),
        );
        assert_eq!(r["status"], "rejected");
    }
    #[test]
    fn timeout_escalates_then_director_approves() {
        let r = run(
            json!({"workflow": wf(), "input": input(900.0), "signals": [sig("s1", 5_000_000, "lee", "director", "approve")]}),
        );
        assert_eq!(r["status"], "completed");
        assert!(r["history"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "Escalated"));
    }
    #[test]
    fn retries_then_compensates() {
        let r = run(
            json!({"workflow": wf(), "input": input(80.0), "fail_times": {"notify_customer": 9}}),
        );
        assert_eq!(r["status"], "failed");
        let h = r["history"].as_array().unwrap();
        assert_eq!(h.iter().filter(|e| e["type"] == "StepFailed").count(), 3);
        assert!(h
            .iter()
            .any(|e| e["type"] == "Compensated" && e["with"] == "reverse_refund"));
    }
    #[test]
    fn crash_anywhere_resumes_identically() {
        for n in 2..20 {
            let r = crash_test_op(&json!({"workflow": wf(), "input": input(900.0),
                "signals": [sig("s1", 60000, "dana", "support_manager", "approve")], "fail_times": {"issue_refund": 1},
                "crash_after_events": n}))
            .unwrap();
            assert_eq!(r["pass"], true, "crash at {n}: {r}");
        }
    }
}
