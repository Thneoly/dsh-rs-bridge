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

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    let mut session_id = String::new();
    let mut next_session = 0_u64;
    let mut pending_prompt: Option<Value> = None;
    // Set while the stashed prompt belongs to the "ignore-cancel" scenario:
    // session/cancel is then deliberately ignored to model a stuck agent.
    let mut ignore_next_cancel = false;

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(_) => break,
        };
        let message: Value = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(_) => continue,
        };
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
