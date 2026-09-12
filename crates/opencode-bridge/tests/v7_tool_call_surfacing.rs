//! T14 / V7 — Tool parts surface as distinct codex item kinds.
//!
//! Verifies the routing in `translate/tool.rs::tool_part_to_item`:
//! - `tool: "bash"` → `commandExecution`
//! - `tool: "write" | "edit" | "patch" | "apply_patch"` → `fileChange`
//! - `tool: "<server>__<name>"` → `mcpToolCall { server, tool: <name> }`
//! - any other tool → `dynamicToolCall { tool }`

#[path = "support/mod.rs"]
mod support;

use serde_json::json;
use support::{FakeServerState, bring_up_bridge, read_until_response, send};

#[tokio::test]
async fn each_tool_kind_routes_to_its_codex_thread_item() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    {
        let mut guard = state.lock().unwrap();
        guard.route(
            "GET /session",
            json!([{
                "id":"ses_v7",
                "directory":"/tmp/v7",
                "title":"V7",
                "time":{"created":1_000,"updated":1_000}
            }]),
        );
        guard.route(
            "GET /session/ses_v7/message",
            json!([{
                "info": {"id":"msg","role":"assistant","sessionID":"ses_v7"},
                "parts": [
                    {
                        "id":"p_bash","type":"tool","callID":"call_bash","tool":"bash",
                        "state":{
                            "status":"completed",
                            "input":{"command":"ls","cwd":"/tmp/v7"},
                            "output":"a\nb\n"
                        }
                    },
                    {
                        "id":"p_write","type":"tool","callID":"call_write","tool":"write",
                        "state":{"status":"completed","input":{"path":"/tmp/v7/x","content":"hi"}}
                    },
                    {
                        "id":"p_mcp","type":"tool","callID":"call_mcp","tool":"server_a__do_thing",
                        "state":{"status":"completed","input":{"x":1}}
                    },
                    {
                        "id":"p_dyn","type":"tool","callID":"call_dyn","tool":"custom_tool",
                        "state":{"status":"completed","input":{}}
                    }
                ]
            }]),
        );
    }

    let mut fx = bring_up_bridge("v7", state.clone()).await;

    send(&mut fx.write, 2, "thread/list", json!({"cwd":"/tmp/v7"})).await;
    let list = read_until_response(&mut fx.read, 2).await;
    let thread_id = list["result"]["data"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send(
        &mut fx.write,
        3,
        "thread/read",
        json!({"threadId":thread_id,"includeTurns":true}),
    )
    .await;
    let read = read_until_response(&mut fx.read, 3).await;
    let items = read["result"]["thread"]["turns"][0]["items"]
        .as_array()
        .expect("items array");
    assert_eq!(items.len(), 4);

    assert_eq!(items[0]["type"], "commandExecution");
    assert_eq!(items[0]["command"], "ls");
    assert_eq!(items[0]["cwd"], "/tmp/v7");
    assert_eq!(items[0]["aggregatedOutput"], "a\nb\n");

    assert_eq!(items[1]["type"], "fileChange");

    assert_eq!(items[2]["type"], "mcpToolCall");
    assert_eq!(items[2]["server"], "server_a");
    assert_eq!(items[2]["tool"], "do_thing");

    assert_eq!(items[3]["type"], "dynamicToolCall");
    assert_eq!(items[3]["tool"], "custom_tool");

    fx.shutdown().await;
}

#[tokio::test]
async fn recent_opencode_tool_shapes_keep_args_results_and_context() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    {
        let mut guard = state.lock().unwrap();
        guard.route(
            "GET /session",
            json!([{
                "id":"ses_v7_real",
                "directory":"/tmp/v7-real",
                "title":"V7 real",
                "time":{"created":1_000,"updated":1_000}
            }]),
        );
        guard.route(
            "GET /session/ses_v7_real/message",
            json!([{
                "info": {"id":"msg","role":"assistant","sessionID":"ses_v7_real"},
                "parts": [
                    {
                        "id":"p_bash","type":"tool","callID":"call_bash","tool":"bash",
                        "state":{
                            "status":"error",
                            "input":{"command":"false"},
                            "output":"bad\n",
                            "metadata":{"exit":2},
                            "time":{"start":100,"end":110}
                        }
                    },
                    {
                        "id":"p_read","type":"tool","callID":"call_read","tool":"read",
                        "state":{
                            "status":"completed",
                            "input":{"filePath":"src/main.rs"},
                            "output":"fn main() {}\n"
                        }
                    },
                    {
                        "id":"p_glob","type":"tool","callID":"call_glob","tool":"glob",
                        "state":{"status":"completed","input":{"pattern":"**/*.rs","path":"src"},"metadata":{"output":"src/main.rs"}}
                    },
                    {
                        "id":"p_list","type":"tool","callID":"call_list","tool":"list",
                        "state":{"status":"completed","input":{"path":"."},"metadata":{"output":"Cargo.toml"}}
                    },
                    {
                        "id":"p_webfetch","type":"tool","callID":"call_webfetch","tool":"webfetch",
                        "state":{"status":"completed","input":{"url":"https://example.com","format":"markdown"},"metadata":{"output":"# Example"}}
                    },
                    {
                        "id":"p_websearch","type":"tool","callID":"call_websearch","tool":"websearch",
                        "state":{"status":"completed","input":{"query":"rust async","numResults":5},"output":"Result text"}
                    },
                    {
                        "id":"p_codesearch","type":"tool","callID":"call_codesearch","tool":"codesearch",
                        "state":{"status":"completed","input":{"query":"symbol:MobileClient","tokensNum":4000},"output":"shared/rust-bridge/codex-mobile-client/src/lib.rs"}
                    },
                    {
                        "id":"p_task","type":"tool","callID":"call_task","tool":"task",
                        "state":{"status":"completed","input":{"prompt":"inspect","subagent_type":"general"},"metadata":{"sessionId":"ses_child","summary":"done"}}
                    },
                    {
                        "id":"p_todo","type":"tool","callID":"call_todo","tool":"todowrite",
                        "state":{"status":"completed","input":{"todos":[{"content":"map tools","status":"completed"}]}}
                    },
                    {
                        "id":"p_question","type":"tool","callID":"call_question","tool":"question",
                        "state":{"status":"completed","input":{"questions":[{"header":"Pick","question":"Choose?"}]},"metadata":{"answers":[["yes"]]}}
                    },
                    {
                        "id":"p_mcp","type":"tool","callID":"call_mcp","tool":"github__create_issue",
                        "state":{"status":"error","input":{"title":"x"},"error":"bad token"}
                    },
                    {
                        "id":"p_patch","type":"patch","hash":"h","files":["src/main.rs"]
                    }
                ]
            }]),
        );
    }

    let mut fx = bring_up_bridge("v7-real", state.clone()).await;

    send(
        &mut fx.write,
        2,
        "thread/list",
        json!({"cwd":"/tmp/v7-real"}),
    )
    .await;
    let list = read_until_response(&mut fx.read, 2).await;
    let thread_id = list["result"]["data"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send(
        &mut fx.write,
        3,
        "thread/read",
        json!({"threadId":thread_id,"includeTurns":true}),
    )
    .await;
    let read = read_until_response(&mut fx.read, 3).await;
    let items = read["result"]["thread"]["turns"][0]["items"]
        .as_array()
        .expect("items array");
    assert_eq!(items.len(), 12);

    assert_eq!(items[0]["type"], "commandExecution");
    assert_eq!(items[0]["cwd"], "/tmp/v7-real");
    assert_eq!(items[0]["status"], "failed");
    assert_eq!(items[0]["exitCode"], 2);
    assert_eq!(items[0]["durationMs"], 10);
    assert_eq!(items[0]["aggregatedOutput"], "bad\n");

    assert_eq!(items[1]["type"], "commandExecution");
    assert_eq!(items[1]["cwd"], "/tmp/v7-real");
    assert_eq!(items[1]["commandActions"][0]["type"], "read");
    assert_eq!(
        items[1]["commandActions"][0]["path"],
        "/tmp/v7-real/src/main.rs"
    );

    assert_eq!(items[2]["commandActions"][0]["type"], "listFiles");
    assert_eq!(items[3]["commandActions"][0]["type"], "listFiles");

    assert_eq!(items[4]["type"], "dynamicToolCall");
    assert_eq!(items[4]["tool"], "webfetch");
    assert_eq!(items[4]["contentItems"][0]["text"], "# Example");

    assert_eq!(items[5]["type"], "webSearch");
    assert_eq!(items[5]["query"], "rust async");
    assert_eq!(items[5]["action"]["type"], "search");

    assert_eq!(items[6]["type"], "commandExecution");
    assert_eq!(items[6]["commandActions"][0]["type"], "search");
    assert_eq!(
        items[6]["commandActions"][0]["query"],
        "symbol:MobileClient"
    );
    assert_eq!(
        items[6]["aggregatedOutput"],
        "shared/rust-bridge/codex-mobile-client/src/lib.rs"
    );

    assert_eq!(items[7]["type"], "collabAgentToolCall");
    assert_eq!(items[7]["receiverThreadIds"][0], "ses_child");
    assert_eq!(items[7]["agentsStates"]["ses_child"]["message"], "done");

    assert_eq!(items[8]["type"], "dynamicToolCall");
    assert_eq!(items[8]["tool"], "todowrite");
    assert_eq!(
        items[8]["contentItems"][0]["text"],
        "- [completed] map tools"
    );

    assert_eq!(items[9]["type"], "dynamicToolCall");
    assert_eq!(items[9]["tool"], "question");
    assert_eq!(
        items[9]["contentItems"][0]["text"],
        "Q: Choose?\nA: [[\"yes\"]]"
    );

    assert_eq!(items[10]["type"], "mcpToolCall");
    assert_eq!(items[10]["status"], "failed");
    assert_eq!(items[10]["error"]["message"], "bad token");

    assert_eq!(items[11]["type"], "fileChange");
    assert_eq!(items[11]["changes"][0]["path"], "src/main.rs");

    fx.shutdown().await;
}
