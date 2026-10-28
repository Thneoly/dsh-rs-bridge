use serde_json::{json, Value};
use std::io::{self, BufRead, Write};

fn send(stdout: &mut impl Write, message: Value) {
    serde_json::to_writer(&mut *stdout, &message).expect("serialize ACP message");
    stdout.write_all(b"\n").expect("write ACP newline");
    stdout.flush().expect("flush ACP output");
}

fn respond(stdout: &mut impl Write, id: Value, result: Value) {
    send(stdout, json!({"jsonrpc":"2.0","id":id,"result":result}));
}

fn session_update(stdout: &mut impl Write, session_id: &str, update: Value) {
    send(
        stdout,
        json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": { "sessionId": session_id, "update": update }
        }),
    );
}

fn chunk(stdout: &mut impl Write, session_id: &str, text: &str) {
    session_update(
        stdout,
        session_id,
        json!({
            "sessionUpdate": "agent_message_chunk",
            "content": { "type": "text", "text": text }
        }),
    );
}

/// Continue a "needs-permission" turn once the client's outcome arrived:
/// an allowed tool call emits the full tool lifecycle; a rejection denies; a
/// cancelled outcome aborts the turn like a spec-conformant agent would.
fn finish_permission_turn(
    stdout: &mut impl Write,
    session_id: &str,
    prompt_id: Value,
    outcome: Option<&str>,
) {
    if matches!(outcome, Some("cancelled")) {
        respond(stdout, prompt_id, json!({"stopReason": "cancelled"}));
        return;
    }
    let allowed = matches!(outcome, Some("allow_once") | Some("allow_always"));
    if allowed {
        session_update(
            stdout,
            session_id,
            json!({
                "sessionUpdate": "tool_call",
                "toolCallId": "fixture-tool-1",
                "title": "read marker file",
                "kind": "read",
                "status": "in_progress",
                "content": []
            }),
        );
        session_update(
            stdout,
            session_id,
            json!({
                "sessionUpdate": "tool_call_update",
                "toolCallId": "fixture-tool-1",
                "title": "read marker file",
                "kind": "read",
                "status": "completed",
                "content": [{ "type": "text", "text": "marker: FIXTURE-MARKER-42" }]
            }),
        );
        chunk(stdout, session_id, "tool ran: FIXTURE-MARKER-42");
    } else {
        chunk(
            stdout,
            session_id,
            &format!(
                "permission denied (outcome: {})",
                outcome.unwrap_or("cancelled")
            ),
        );
    }
    respond(stdout, prompt_id, json!({"stopReason": "end_turn"}));
}

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut session_id = String::new();
    let mut next_session = 0_u64;
    let mut pending_prompt: Option<Value> = None;
    // Set while the stashed prompt belongs to the "ignore-cancel" scenario:
    // session/cancel is then deliberately ignored to model a stuck agent.
    let mut ignore_next_cancel = false;
    // Set after sending session/request_permission: the prompt request id to
    // answer once the client's outcome arrives on a later stdin line.
    let mut awaiting_permission: Option<Value> = None;
    let mut permission_outcome: Option<String> = None;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        let message: Value = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(_) => continue,
        };
        // Responses to requests we sent (the permission outcome).
        if message.get("method").is_none() {
            if let Some(outcome) = message.get("result").and_then(|r| r.get("outcome")) {
                permission_outcome = outcome
                    .get("optionId")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| {
                        outcome
                            .get("outcome")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    });
            }
            if let Some(prompt_id) = awaiting_permission.take() {
                finish_permission_turn(
                    &mut stdout,
                    &session_id,
                    prompt_id,
                    permission_outcome.as_deref(),
                );
                permission_outcome = None;
            }
            continue;
        }
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let id = message.get("id").cloned();

        match method {
            "initialize" => {
                if let Some(id) = id {
                    respond(
                        &mut stdout,
                        id,
                        json!({
                            "protocolVersion": 1,
                            "agentInfo": {"name": "acp-test-agent", "version": "0.1.0"},
                            "agentCapabilities": {
                                "loadSession": true,
                                "sessionCapabilities": { "resume": {} }
                            }
                        }),
                    );
                }
            }
            "session/new" => {
                next_session += 1;
                session_id = format!("fixture-session-{next_session}");
                if let Some(id) = id {
                    respond(&mut stdout, id, json!({"sessionId":session_id.clone()}));
                }
            }
            "session/resume" => {
                let requested_session = message["params"]["sessionId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                if requested_session.trim().is_empty() {
                    if let Some(id) = id {
                        send(
                            &mut stdout,
                            json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "error": { "code": -32602, "message": "sessionId is required" }
                            }),
                        );
                    }
                    continue;
                }
                session_id = requested_session;
                if let Some(id) = id {
                    respond(&mut stdout, id, json!({}));
                }
            }
            "session/prompt" => {
                let prompt_session = message["params"]["sessionId"].as_str().unwrap_or_default();
                let prompt = message["params"]["prompt"]
                    .as_array()
                    .and_then(|blocks| blocks.first())
                    .and_then(|block| block["text"].as_str())
                    .unwrap_or_default()
                    .to_owned();
                let Some(id) = id else { continue };
                if prompt_session != session_id.as_str() {
                    send(
                        &mut stdout,
                        json!({"jsonrpc":"2.0","id":id,
                        "error":{"code":-32001,"message":"unknown session"}}),
                    );
                    continue;
                }
                if prompt == "cancel-me" {
                    pending_prompt = Some(id);
                    continue;
                }
                if prompt == "needs-permission" {
                    // Ask the client whether the tool may run; the turn
                    // continues when the outcome arrives on a later line.
                    send(
                        &mut stdout,
                        json!({
                            "jsonrpc": "2.0",
                            "id": "perm-1",
                            "method": "session/request_permission",
                            "params": {
                                "sessionId": session_id.clone(),
                                "toolCall": {
                                    "toolCallId": "fixture-tool-1",
                                    "title": "read marker file",
                                    "kind": "read",
                                    "status": "in_progress"
                                },
                                "options": [
                                    { "optionId": "allow_once", "name": "Allow", "kind": "allow_once" },
                                    { "optionId": "reject_once", "name": "Deny", "kind": "reject_once" }
                                ]
                            }
                        }),
                    );
                    awaiting_permission = Some(id);
                    continue;
                }
                if prompt == "crash" {
                    // Die mid-turn without answering the prompt.
                    send(
                        &mut stdout,
                        json!({
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {
                                "sessionId": session_id.clone(),
                                "update": {
                                    "sessionUpdate": "agent_message_chunk",
                                    "content": {"type": "text", "text": "ACP is about to crash"}
                                }
                            }
                        }),
                    );
                    let _ = stdout.flush();
                    std::process::exit(1);
                }
                if prompt == "garbage" {
                    // Emit a corrupt stdout line mid-turn, then behave
                    // normally so the session stays usable afterwards.
                    let _ = stdout.write_all(b"this line is not json\n");
                    let _ = stdout.flush();
                }
                if prompt == "wedge-now" {
                    // Answer this turn, then stop draining stdin forever:
                    // an agent whose pipe the host can no longer write to.
                    respond(&mut stdout, id, json!({"stopReason":"end_turn"}));
                    let _ = stdout.flush();
                    loop {
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
                if prompt == "ignore-cancel" {
                    // Stream one chunk, then never answer the prompt and
                    // ignore session/cancel: a stuck agent.
                    pending_prompt = Some(id);
                    ignore_next_cancel = true;
                    send(
                        &mut stdout,
                        json!({
                            "jsonrpc": "2.0",
                            "method": "session/update",
                            "params": {
                                "sessionId": session_id.clone(),
                                "update": {
                                    "sessionUpdate": "agent_message_chunk",
                                    "content": {"type": "text", "text": "ACP is stuck"}
                                }
                            }
                        }),
                    );
                    continue;
                }
                send(
                    &mut stdout,
                    json!({
                        "jsonrpc": "2.0",
                        "method": "session/update",
                        "params": {
                            "sessionId": session_id.clone(),
                            "update": {
                                "sessionUpdate": "agent_message_chunk",
                                "content": {
                                    "type": "text",
                                    "text": format!("ACP says: {prompt}")
                                }
                            }
                        }
                    }),
                );
                respond(&mut stdout, id, json!({"stopReason":"end_turn"}));
            }
            "session/cancel" => {
                if ignore_next_cancel {
                    continue;
                }
                if let Some(id) = pending_prompt.take() {
                    respond(&mut stdout, id, json!({"stopReason":"cancelled"}));
                }
            }
            "session/close" => {
                if let Some(id) = id {
                    respond(&mut stdout, id, json!({}));
                }
            }
            _ => {
                if let Some(id) = id {
                    send(
                        &mut stdout,
                        json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {"code": -32601, "message": "method not found"}
                        }),
                    );
                }
            }
        }
    }
}
