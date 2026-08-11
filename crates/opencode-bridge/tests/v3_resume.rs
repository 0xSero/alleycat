//! T14 / V3-resume — `thread/list` then `thread/resume` then
//! `thread/read{includeTurns:true}` reconstruct a session's prior turns from
//! opencode's stored messages.
//!
//! Drives the parts-driven path through `message_to_turn_items` (independent
//! of T3's live message lifecycle), so the assertion is on the
//! ThreadItem array shape returned by `thread/read`.

#[path = "support/mod.rs"]
mod support;

use serde_json::json;
use support::{FakeServerState, bring_up_bridge, read_until_response, send};

#[tokio::test]
async fn thread_resume_then_read_returns_persisted_turns() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    {
        let mut guard = state.lock().unwrap();
        // The session lookup served by `thread/list { cwd }`.
        guard.route(
            "GET /session",
            json!([{
                "id":"ses_resume",
                "directory":"/tmp/v3r",
                "title":"V3-Resume",
                "time":{"created":1_000,"updated":1_000}
            }]),
        );
        // `thread/resume` and `thread/read` both call GET /session/{id}/message.
        guard.route(
            "GET /session/ses_resume/message",
            json!([
                {
                    "info": {"id":"msg_user","role":"user","sessionID":"ses_resume"},
                    "parts": [{"id":"p1","type":"text","text":"hello"}]
                },
                {
                    "info": {"id":"msg_asst","role":"assistant","sessionID":"ses_resume"},
                    "parts": [
                        {"id":"p2","type":"text","text":"hi back"},
                        {"id":"p3","type":"reasoning","text":"thinking"}
                    ]
                }
            ]),
        );
    }

    let mut fx = bring_up_bridge("v3r", state.clone()).await;

    // thread/list { cwd } binds the existing ses_resume session into the index.
    send(&mut fx.write, 2, "thread/list", json!({"cwd":"/tmp/v3r"})).await;
    let list = read_until_response(&mut fx.read, 2).await;
    let listed = list["result"]["data"].as_array().expect("data array");
    assert_eq!(listed.len(), 1);
    let thread_id = listed[0]["id"].as_str().unwrap().to_string();

    // thread/resume returns the same thread plus model/cwd metadata.
    send(
        &mut fx.write,
        3,
        "thread/resume",
        json!({"threadId":thread_id}),
    )
    .await;
    let resume = read_until_response(&mut fx.read, 3).await;
    assert_eq!(
        resume["result"]["thread"]["id"].as_str(),
        Some(thread_id.as_str())
    );
    assert_eq!(resume["result"]["cwd"], "/tmp/v3r");
    let turns = resume["result"]["thread"]["turns"]
        .as_array()
        .expect("turns array");
    assert_eq!(turns.len(), 1);

    // thread/read{includeTurns:true} returns Codex-style turns: one user
    // prompt plus all assistant-side items that answered it.
    send(
        &mut fx.write,
        4,
        "thread/read",
        json!({"threadId":thread_id,"includeTurns":true}),
    )
    .await;
    let read = read_until_response(&mut fx.read, 4).await;
    let turns = read["result"]["thread"]["turns"].as_array().expect("turns");
    assert_eq!(turns.len(), 1);
    let items = turns[0]["items"].as_array().expect("turn items");
    let kinds: Vec<&str> = items
        .iter()
        .map(|it| it["type"].as_str().unwrap_or(""))
        .collect();
    assert_eq!(kinds, vec!["userMessage", "agentMessage", "reasoning"]);
    assert_eq!(items[0]["content"][0]["text"], "hello");
    assert_eq!(items[1]["text"], "hi back");
    assert_eq!(items[2]["content"][0], "thinking");

    fx.shutdown().await;
}

#[tokio::test]
async fn thread_resume_with_bash_history_keeps_command_actions_field() {
    let state = std::sync::Arc::new(std::sync::Mutex::new(FakeServerState::default()));
    {
        let mut guard = state.lock().unwrap();
        guard.route(
            "GET /session",
            json!([{
                "id":"ses_resume_bash",
                "directory":"/tmp/v3r-bash",
                "title":"V3-Resume Bash",
                "time":{"created":1_000,"updated":1_000}
            }]),
        );
        guard.route(
            "GET /session/ses_resume_bash/message",
            json!([
                {
                    "info": {"id":"msg_user","role":"user","sessionID":"ses_resume_bash"},
                    "parts": [{"id":"p1","type":"text","text":"run ls"}]
                },
                {
                    "info": {"id":"msg_asst","role":"assistant","sessionID":"ses_resume_bash"},
                    "parts": [{
                        "id":"p_bash","type":"tool","callID":"call_bash","tool":"bash",
                        "state":{
                            "status":"completed",
                            "input":{"command":"ls","cwd":"/tmp/v3r-bash"},
                            "output":"a\nb\n"
                        }
                    }]
                }
            ]),
        );
    }

    let mut fx = bring_up_bridge("v3r-bash", state.clone()).await;

    send(
        &mut fx.write,
        2,
        "thread/list",
        json!({"cwd":"/tmp/v3r-bash"}),
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
        "thread/resume",
        json!({"threadId":thread_id}),
    )
    .await;
    let resume = read_until_response(&mut fx.read, 3).await;
    let turns = resume["result"]["thread"]["turns"]
        .as_array()
        .expect("turns array");
    assert_eq!(turns.len(), 1);
    let items = turns[0]["items"].as_array().expect("turn items");
    assert_eq!(items.len(), 2);
    assert_eq!(items[1]["type"], "commandExecution");
    assert_eq!(items[1]["command"], "ls");
    assert_eq!(items[1]["commandActions"], json!([]));

    fx.shutdown().await;
}
