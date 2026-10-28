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
