use std::io::{self, BufRead, Write};

use serde_json::{Value, json};

const FACTORY_API_VERSION: &str = "1.0.0";
const FACTORY_PROTOCOL_VERSION: &str = "1.36.0";

fn main() {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut settings = json!({"modelId":"gpt-5.6-sol", "reasoningEffort":"medium"});
    for line in stdin.lock().lines().map_while(Result::ok) {
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let id = frame.get("id").cloned().unwrap_or(Value::Null);
        let method = frame.get("method").and_then(Value::as_str).unwrap_or("");
        let params = frame.get("params").cloned().unwrap_or_else(|| json!({}));
        match method {
            "droid.initialize_session" | "droid.load_session" => {
                if let Some(model) = params.get("modelId").and_then(Value::as_str) {
                    settings["modelId"] = json!(model);
                }
                response(
                    &mut stdout,
                    id,
                    json!({"sessionId": params.get("sessionId"), "settings": settings}),
                );
            }
            "droid.list_models" => {
                response(&mut stdout, id, serde_json::from_str(include_str!("../fixtures/models.json")).unwrap());
            }
            "droid.update_session_settings" => {
                for key in ["modelId", "reasoningEffort"] {
                    if let Some(value) = params.get(key) { settings[key] = value.clone(); }
                }
                response(&mut stdout, id, json!({"settings":settings}));
            }
            "droid.list_tools" => {
                response(
                    &mut stdout,
                    id,
                    json!({
                        "tools": [{
                            "name": "Execute",
                            "description": "Run a shell command",
                            "inputSchema": {"type":"object"}
                        }]
                    }),
                );
            }
            "droid.add_user_message" => {
                let prompt = prompt_text(&params);
                if prompt.contains("check-native-model-effort") && (settings["modelId"] != "claude-opus-5" || settings["reasoningEffort"] != "max") {
                    error(&mut stdout, id, -32602, "model/effort settings did not reach native runtime");
                    continue;
                }
                response(&mut stdout, id, json!({}));
                scripted_turn(&mut stdout, &prompt);
            }
            "droid.interrupt_session" | "droid.rename_session" => {
                response(&mut stdout, id, json!({}));
            }
            _ => {
                error(&mut stdout, id, -32601, &format!("unknown method {method}"));
            }
        }
        let _ = stdout.flush();
    }
}

fn scripted_turn(stdout: &mut io::Stdout, prompt: &str) {
    notification(
        stdout,
        json!({"type":"droid_working_state_changed","newState":"working"}),
    );
    notification(
        stdout,
        json!({
            "type":"create_message",
            "message":{
                "id":"user_1",
                "role":"user",
                "content":[{"type":"text","text": prompt}]
            }
        }),
    );
    if prompt.contains("conformance-marker") || prompt.contains("cat ") {
        notification(
            stdout,
            json!({
                "type":"tool_call",
                "toolUse":{
                    "id":"tool_1",
                    "name":"Execute",
                    "input":{"command":"cat conformance-marker.txt"}
                }
            }),
        );
        notification(
            stdout,
            json!({
                "type":"tool_progress_update",
                "toolUseId":"tool_1",
                "update":{"fullOutput":"alleycat-marker"}
            }),
        );
        notification(
            stdout,
            json!({
                "type":"tool_result",
                "toolUseId":"tool_1",
                "toolName":"Execute",
                "content":"alleycat-marker\n[Process exited with code 0]",
                "isError":false
            }),
        );
        assistant(stdout, "The literal contents are alleycat-marker.");
    } else {
        assistant(stdout, "OK");
    }
    notification(
        stdout,
        json!({
            "type":"session_token_usage_changed",
            "tokenUsage":{"inputTokens":1,"outputTokens":1},
            "lastCallTokenUsage":{"inputTokens":1,"outputTokens":1}
        }),
    );
    notification(
        stdout,
        json!({"type":"session_title_updated","title":"Fake Droid"}),
    );
    notification(
        stdout,
        json!({"type":"droid_working_state_changed","newState":"idle"}),
    );
}

fn assistant(stdout: &mut io::Stdout, text: &str) {
    notification(
        stdout,
        json!({
            "type":"assistant_text_delta",
            "messageId":"assistant_1",
            "textDelta": text
        }),
    );
    notification(
        stdout,
        json!({
            "type":"assistant_text_complete",
            "messageId":"assistant_1"
        }),
    );
    notification(
        stdout,
        json!({
            "type":"create_message",
            "message":{
                "id":"assistant_1",
                "role":"assistant",
                "content":[{"type":"text","text": text}]
            }
        }),
    );
}

fn prompt_text(params: &Value) -> String {
    if let Some(message) = params.get("message") {
        if let Some(text) = message.as_str() {
            return text.to_string();
        }
        if let Some(text) = message.get("text").and_then(Value::as_str) {
            return text.to_string();
        }
        if let Some(content) = message.get("content").and_then(Value::as_array) {
            return content
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
        }
    }
    params
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

fn response(stdout: &mut io::Stdout, id: Value, result: Value) {
    write_frame(
        stdout,
        json!({
            "type":"response",
            "jsonrpc":"2.0",
            "factoryApiVersion": FACTORY_API_VERSION,
            "factoryProtocolVersion": FACTORY_PROTOCOL_VERSION,
            "id": id,
            "result": result
        }),
    );
}

fn error(stdout: &mut io::Stdout, id: Value, code: i64, message: &str) {
    write_frame(
        stdout,
        json!({
            "type":"response",
            "jsonrpc":"2.0",
            "factoryApiVersion": FACTORY_API_VERSION,
            "factoryProtocolVersion": FACTORY_PROTOCOL_VERSION,
            "id": id,
            "error": {"code": code, "message": message}
        }),
    );
}

fn notification(stdout: &mut io::Stdout, notification: Value) {
    write_frame(
        stdout,
        json!({
            "type":"notification",
            "jsonrpc":"2.0",
            "factoryApiVersion": FACTORY_API_VERSION,
            "factoryProtocolVersion": FACTORY_PROTOCOL_VERSION,
            "method":"droid.session_notification",
            "params":{"notification": notification}
        }),
    );
}

fn write_frame(stdout: &mut io::Stdout, frame: Value) {
    let _ = writeln!(stdout, "{}", serde_json::to_string(&frame).unwrap());
}
