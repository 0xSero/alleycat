use serde_json::{Value, json};

use super::tool::{ToolPartContext, tool_part_to_item_with_context};

pub fn message_to_turn_items(message: &Value) -> Vec<Value> {
    message_to_turn_items_with_context(message, ToolPartContext::default())
}

pub fn message_to_turn_items_with_context(
    message: &Value,
    tool_context: ToolPartContext<'_>,
) -> Vec<Value> {
    let mut items = Vec::new();
    let role = message
        .pointer("/info/role")
        .and_then(Value::as_str)
        .unwrap_or("assistant");
    let sender_thread_id = tool_context
        .sender_thread_id
        .or_else(|| message.pointer("/info/sessionID").and_then(Value::as_str))
        .unwrap_or("");
    for part in message
        .get("parts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(item) = part_to_item_with_context(part, role, sender_thread_id, tool_context) {
            items.push(item);
        }
    }
    items
}

pub fn part_to_item(part: &Value, role: &str, sender_thread_id: &str) -> Option<Value> {
    part_to_item_with_context(part, role, sender_thread_id, ToolPartContext::default())
}

pub fn part_to_item_with_context(
    part: &Value,
    role: &str,
    sender_thread_id: &str,
    tool_context: ToolPartContext<'_>,
) -> Option<Value> {
    let id = part
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("opencode-part");
    match part.get("type").and_then(Value::as_str)? {
        "text" => {
            let text = part.get("text").and_then(Value::as_str).unwrap_or("");
            if role == "user" {
                // Codex wire shape for the user prompt: a `userMessage` item
                // whose `content` is a list of `UserInput` blocks. Opencode
                // stores the prompt as a plain text part on a `role: "user"`
                // message, so we lift it into the codex envelope here.
                Some(json!({
                    "type": "userMessage",
                    "id": id,
                    "content": [{
                        "type": "text",
                        "text": text,
                        "text_elements": []
                    }]
                }))
            } else {
                Some(json!({
                    "type": "agentMessage",
                    "id": id,
                    "text": text,
                    "phase": "final_answer",
                    "memoryCitation": null,
                }))
            }
        }
        "reasoning" => Some(
            json!({"type":"reasoning","id":id,"summary":[],"content":[part.get("text").and_then(Value::as_str).unwrap_or("")]}),
        ),
        "compaction" => Some(json!({"type":"contextCompaction","id":id})),
        "tool" => tool_part_to_item_with_context(part, tool_context),
        "file" => file_part_to_item(part, id, role),
        "agent" => agent_part_to_item(part, id, role),
        "subtask" => Some(subtask_part_to_item(part, id, sender_thread_id)),
        "patch" => patch_part_to_item(part, id),
        // RetryPart / StepStartPart / StepFinishPart / SnapshotPart
        // are not surfaced as ThreadItems. Token usage is derived from
        // step-finish elsewhere; retries are an SSE-level concern; snapshots
        // fold into sibling FileChange items emitted by tool calls.
        "retry" | "step-start" | "step-finish" | "snapshot" => None,
        _ => None,
    }
}

fn file_part_to_item(part: &Value, id: &str, role: &str) -> Option<Value> {
    let mime = part.get("mime").and_then(Value::as_str).unwrap_or("");
    let url = part.get("url").and_then(Value::as_str).unwrap_or("");
    if role == "user" {
        // User-side file parts are typically attachments. Render as a
        // user-message item containing a single Image UserInput when the
        // mime is image-shaped; otherwise emit a text breadcrumb.
        if mime.starts_with("image/") {
            return Some(json!({
                "type": "userMessage",
                "id": id,
                "content": [{"type":"image","url":url}]
            }));
        }
        let filename = part
            .get("filename")
            .and_then(Value::as_str)
            .unwrap_or("file");
        return Some(json!({
            "type": "userMessage",
            "id": id,
            "content": [{"type":"text","text":format!("[file: {filename}]"),"textElements":[]}]
        }));
    }
    // Assistant-side: only `file://` URLs translate to `ImageView` (which
    // requires an absolute path). For data: URLs we fall back to a text
    // breadcrumb on `agentMessage` so the client at least sees something.
    if let Some(path) = url.strip_prefix("file://")
        && mime.starts_with("image/")
    {
        return Some(json!({"type":"imageView","id":id,"path":path}));
    }
    let filename = part
        .get("filename")
        .and_then(Value::as_str)
        .unwrap_or("file");
    Some(json!({
        "type": "agentMessage",
        "id": id,
        "text": format!("[file: {filename} ({mime})]"),
        "phase": "final_answer",
        "memoryCitation": null,
    }))
}

fn agent_part_to_item(part: &Value, id: &str, role: &str) -> Option<Value> {
    let name = part.get("name").and_then(Value::as_str).unwrap_or("agent");
    if role == "user" {
        let path = part
            .pointer("/source/value")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let mut skill = json!({"type":"skill","name":name});
        if let Some(path) = path {
            skill["path"] = json!(path);
        }
        return Some(json!({
            "type": "userMessage",
            "id": id,
            "content": [skill]
        }));
    }
    Some(json!({
        "type": "agentMessage",
        "id": id,
        "text": format!("[skill: {name}]"),
        "phase": "final_answer",
        "memoryCitation": null,
    }))
}

fn patch_part_to_item(part: &Value, id: &str) -> Option<Value> {
    let files = part.get("files").and_then(Value::as_array)?;
    if files.is_empty() {
        return None;
    }
    let changes: Vec<Value> = files
        .iter()
        .filter_map(|file| {
            let path = file
                .as_str()
                .or_else(|| file.get("path").and_then(Value::as_str))
                .or_else(|| file.get("file").and_then(Value::as_str))?;
            Some(json!({
                "path": path,
                "kind": {"type": "update"},
                "diff": file
                    .get("patch")
                    .or_else(|| file.get("diff"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
            }))
        })
        .collect();
    if changes.is_empty() {
        return None;
    }
    Some(json!({
        "type": "fileChange",
        "id": id,
        "changes": changes,
        "status": "completed",
    }))
}

fn subtask_part_to_item(part: &Value, id: &str, sender_thread_id: &str) -> Value {
    let agent = part.get("agent").and_then(Value::as_str).unwrap_or("agent");
    let prompt = part
        .get("prompt")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let model = part
        .pointer("/model/modelID")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let mut item = json!({
        "type": "collabAgentToolCall",
        "id": id,
        "tool": agent,
        "status": "completed",
        "senderThreadId": sender_thread_id,
        "receiverThreadIds": [],
        "agentsStates": {}
    });
    if let Some(prompt) = prompt {
        item["prompt"] = json!(prompt);
    }
    if let Some(model) = model {
        item["model"] = json!(model);
    }
    item
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message(role: &str, parts: Vec<Value>) -> Value {
        json!({
            "info": {"id":"msg_x","role":role,"sessionID":"ses_x"},
            "parts": parts
        })
    }

    #[test]
    fn covers_full_part_union() {
        let message = make_message(
            "assistant",
            vec![
                json!({"id":"p1","type":"text","text":"hi"}),
                json!({"id":"p2","type":"reasoning","text":"think"}),
                json!({"id":"p3","type":"compaction","auto":false}),
                json!({"id":"p4","type":"tool","callID":"c4","tool":"bash","state":{"status":"completed","input":{"command":"ls"},"output":"a"}}),
                json!({"id":"p5","type":"file","mime":"image/png","url":"file:///tmp/a.png"}),
                json!({"id":"p6","type":"agent","name":"reviewer"}),
                json!({"id":"p7","type":"subtask","prompt":"go","agent":"worker","description":"d"}),
                json!({"id":"p8","type":"retry","attempt":1,"error":{},"time":{"created":0}}),
                json!({"id":"p9","type":"step-start"}),
                json!({"id":"p10","type":"step-finish","reason":"end","cost":0,"tokens":{"input":0,"output":0,"reasoning":0,"cache":{"read":0,"write":0}}}),
                json!({"id":"p11","type":"snapshot","snapshot":"s"}),
                json!({"id":"p12","type":"patch","hash":"h","files":[]}),
            ],
        );
        let items = message_to_turn_items(&message);
        let types: Vec<&str> = items
            .iter()
            .map(|item| item["type"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(
            types,
            vec![
                "agentMessage",
                "reasoning",
                "contextCompaction",
                "commandExecution",
                "imageView",
                "agentMessage",
                "collabAgentToolCall",
            ]
        );
        assert_eq!(items[4]["path"], "/tmp/a.png");
        assert_eq!(items[6]["tool"], "worker");
        assert_eq!(items[6]["prompt"], "go");
        assert_eq!(items[6]["senderThreadId"], "ses_x");
    }

    #[test]
    fn user_file_image_renders_as_user_input_image() {
        let message = make_message(
            "user",
            vec![
                json!({"id":"p1","type":"file","mime":"image/jpeg","url":"data:image/jpeg;base64,Zm9v"}),
            ],
        );
        let items = message_to_turn_items(&message);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "userMessage");
        assert_eq!(items[0]["content"][0]["type"], "image");
        assert_eq!(items[0]["content"][0]["url"], "data:image/jpeg;base64,Zm9v");
    }

    #[test]
    fn user_text_part_emits_user_message_with_text_elements() {
        let message = make_message(
            "user",
            vec![json!({"id":"p1","type":"text","text":"hello"})],
        );
        let items = message_to_turn_items(&message);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "userMessage");
        assert_eq!(items[0]["content"][0]["type"], "text");
        assert_eq!(items[0]["content"][0]["text"], "hello");
        assert_eq!(items[0]["content"][0]["text_elements"], json!([]));
    }

    #[test]
    fn assistant_text_part_still_emits_agent_message() {
        let message = make_message(
            "assistant",
            vec![json!({"id":"p1","type":"text","text":"reply"})],
        );
        let items = message_to_turn_items(&message);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "agentMessage");
        assert_eq!(items[0]["text"], "reply");
    }

    #[test]
    fn history_tool_parts_use_context_and_side_channel_items() {
        let message = make_message(
            "assistant",
            vec![
                json!({"id":"p1","type":"tool","callID":"c1","tool":"bash","state":{"status":"completed","input":{"command":"pwd"},"output":"/repo\n"}}),
                json!({"id":"p2","type":"tool","callID":"c2","tool":"todowrite","state":{"status":"completed","input":{"todos":[{"content":"map tools","status":"completed"}]}}}),
                json!({"id":"p3","type":"tool","callID":"c3","tool":"task","state":{"status":"completed","input":{"prompt":"go"},"metadata":{"sessionId":"ses_child"}}}),
            ],
        );
        let items = message_to_turn_items_with_context(
            &message,
            ToolPartContext {
                cwd: Some("/repo"),
                sender_thread_id: Some("thread_parent"),
                include_side_channel_items: true,
            },
        );
        assert_eq!(items[0]["type"], "commandExecution");
        assert_eq!(items[0]["cwd"], "/repo");
        assert_eq!(items[1]["type"], "dynamicToolCall");
        assert_eq!(items[1]["tool"], "todowrite");
        assert_eq!(
            items[1]["contentItems"][0]["text"],
            "- [completed] map tools"
        );
        assert_eq!(items[2]["type"], "collabAgentToolCall");
        assert_eq!(items[2]["senderThreadId"], "thread_parent");
        assert_eq!(items[2]["receiverThreadIds"][0], "ses_child");
    }

    #[test]
    fn patch_part_with_files_surfaces_file_change() {
        let message = make_message(
            "assistant",
            vec![json!({"id":"p1","type":"patch","hash":"h","files":[
                "src/lib.rs",
                {"file":"README.md","patch":"@@ -1 +1 @@\n-old\n+new\n"}
            ]})],
        );
        let items = message_to_turn_items(&message);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["type"], "fileChange");
        assert_eq!(items[0]["status"], "completed");
        assert_eq!(items[0]["changes"][0]["path"], "src/lib.rs");
        assert_eq!(items[0]["changes"][1]["path"], "README.md");
        assert_eq!(items[0]["changes"][1]["diff"], "@@ -1 +1 @@\n-old\n+new\n");
    }

    #[test]
    fn user_agent_part_renders_as_skill() {
        let message = make_message(
            "user",
            vec![
                json!({"id":"p1","type":"agent","name":"reviewer","source":{"value":"@reviewer","start":0,"end":9}}),
            ],
        );
        let items = message_to_turn_items(&message);
        assert_eq!(items[0]["content"][0]["type"], "skill");
        assert_eq!(items[0]["content"][0]["name"], "reviewer");
        assert_eq!(items[0]["content"][0]["path"], "@reviewer");
    }
}
