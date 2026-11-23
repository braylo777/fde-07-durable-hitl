//! Approval requests for Slack and Microsoft Teams, and Slack callback verification.

use crate::str_field;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;

pub fn message_op(i: &Value) -> Result<Value, String> {
    let channel = str_field(i, "channel")?;
    let wf = str_field(i, "workflow_id")?;
    let step = str_field(i, "step")?;
    let summary = str_field(i, "summary")?;
    let facts: Vec<(String, String)> = i["facts"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_str().map(String::from).unwrap_or(v.to_string()),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let value = json!({"workflow_id": wf, "step": step}).to_string();
    Ok(match channel {
        "slack" => json!({"blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": "Approval needed"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": summary}},
            {"type": "section", "fields": facts.iter().map(|(k, v)| json!({"type": "mrkdwn", "text": format!("*{k}*\n{v}")})).collect::<Vec<_>>()},
            {"type": "actions", "block_id": format!("{wf}:{step}"), "elements": [
                {"type": "button", "style": "primary", "text": {"type": "plain_text", "text": "Approve"}, "action_id": "approve", "value": value},
                {"type": "button", "style": "danger", "text": {"type": "plain_text", "text": "Deny"}, "action_id": "deny", "value": value,
                 "confirm": {"title": {"type": "plain_text", "text": "Deny this request?"}, "text": {"type": "plain_text", "text": "The workflow will stop and the requester is told."},
                             "confirm": {"type": "plain_text", "text": "Deny"}, "deny": {"type": "plain_text", "text": "Cancel"}}}]},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": format!("Workflow `{wf}` · step `{step}` · your decision is recorded in the audit trail")}]}]}),
        "teams" => {
            json!({"type": "message", "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "type": "AdaptiveCard", "version": "1.5",
            "body": [
                {"type": "TextBlock", "text": "Approval needed", "weight": "Bolder", "size": "Medium"},
                {"type": "TextBlock", "text": summary, "wrap": true},
                {"type": "FactSet", "facts": facts.iter().map(|(k, v)| json!({"title": k, "value": v})).collect::<Vec<_>>()},
                {"type": "Input.Text", "id": "reason", "placeholder": "Reason (required to deny)", "isMultiline": true}],
            "actions": [
                {"type": "Action.Execute", "title": "Approve", "verb": "approve", "data": {"workflow_id": wf, "step": step}, "style": "positive"},
                {"type": "Action.Execute", "title": "Deny", "verb": "deny", "data": {"workflow_id": wf, "step": step}, "style": "destructive"}]}}]})
        }
        other => return Err(format!("channel must be slack or teams, not {other}")),
    })
}

/// Slack signing: v0=hex(HMAC_SHA256(secret, "v0:{timestamp}:{body}")); reject if older than 5 min.
pub fn verify_op(i: &Value) -> Result<Value, String> {
    let secret = str_field(i, "signing_secret")?;
    let ts = str_field(i, "timestamp")?;
    let body = str_field(i, "body")?;
    let sig = str_field(i, "signature")?;
    let now = i["now"].as_u64().ok_or("missing `now` (unix seconds)")?;
    let t: u64 = ts.parse().map_err(|_| "timestamp must be unix seconds")?;
    if now.abs_diff(t) > 300 {
        return Ok(
            json!({"valid": false, "reason": "timestamp outside 5-minute window (replay?)"}),
        );
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("v0:{ts}:{body}").as_bytes());
    let Some(hex) = sig.strip_prefix("v0=") else {
        return Ok(json!({"valid": false, "reason": "signature must start with v0="}));
    };
    let bytes: Result<Vec<u8>, _> = (0..hex.len())
        .step_by(2)
        .map(|k| u8::from_str_radix(hex.get(k..k + 2).unwrap_or("zz"), 16))
        .collect();
    let ok = bytes.map(|b| mac.verify_slice(&b).is_ok()).unwrap_or(false); // constant-time compare
    Ok(json!({"valid": ok, "reason": if ok { "signature matches" } else { "signature mismatch" }}))
}

pub fn sign(secret: &str, ts: &str, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("v0:{ts}:{body}").as_bytes());
    format!(
        "v0={}",
        mac.finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn slack_signature() {
        let s = sign("sekret", "1700000000", "payload=x");
        let ok = verify_op(&json!({"signing_secret": "sekret", "timestamp": "1700000000", "body": "payload=x", "signature": s, "now": 1700000100})).unwrap();
        assert_eq!(ok["valid"], true);
        let bad = verify_op(&json!({"signing_secret": "sekret", "timestamp": "1700000000", "body": "payload=y", "signature": s, "now": 1700000100})).unwrap();
        assert_eq!(bad["valid"], false);
        let old = verify_op(&json!({"signing_secret": "sekret", "timestamp": "1700000000", "body": "payload=x", "signature": s, "now": 1700009999})).unwrap();
        assert_eq!(old["valid"], false);
    }
    #[test]
    fn messages_render() {
        let s = message_op(&json!({"channel": "slack", "workflow_id": "w", "step": "s", "summary": "x", "facts": {"Amount": "$900"}})).unwrap();
        assert_eq!(s["blocks"][3]["elements"][0]["action_id"], "approve");
        let t = message_op(
            &json!({"channel": "teams", "workflow_id": "w", "step": "s", "summary": "x"}),
        )
        .unwrap();
        assert_eq!(t["attachments"][0]["content"]["type"], "AdaptiveCard");
    }
}
