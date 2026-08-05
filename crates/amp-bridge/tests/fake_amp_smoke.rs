#![cfg(unix)]

use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration;

use alleycat_amp_bridge::AmpBridge;
use alleycat_bridge_core::{
    ChildProcess, ChildStderr, ChildStdin, ChildStdout, LocalLauncher, ProcessLauncher,
    ProcessSpec, serve_stream,
};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream, ReadBuf,
};
use tokio::sync::Notify;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[tokio::test]
async fn fake_amp_turn_persists_thread_and_emits_lifecycle() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp.sh");
    std::fs::write(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
continuing=0
case " $* " in
  *" threads continue T-fake-amp "*) continuing=1 ;;
esac
case " $* " in
  *" --mode medium "*) ;;
  *) echo "missing --mode medium in amp argv: $*" >&2; exit 7 ;;
esac
case " $* " in
  *" --effort "*) echo "unexpected --effort in amp argv: $*" >&2; exit 8 ;;
esac
case " $* " in
  *" --stream-json-thinking "*) ;;
  *) echo "missing --stream-json-thinking in amp argv: $*" >&2; exit 9 ;;
esac
first=""
if IFS= read -r first; then
  :
fi
if [[ "$continuing" == "1" ]]; then
  text="second from fake amp"
else
  text="hello from fake amp"
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-fake-amp","tools":["Bash"],"mcp_servers":[],"agent_mode":"medium","future_field":{"safe_to_ignore":true}}'
printf '{"type":"user","message":{"role":"user","content":[{"type":"text","text":"hello amp"}]},"parent_tool_use_id":null,"session_id":"T-fake-amp"}\n'
printf '{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"%s"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}},"parent_tool_use_id":null,"session_id":"T-fake-amp"}\n' "$text"
cat >/dev/null || true
printf '{"type":"result","subtype":"success","duration_ms":42,"is_error":false,"num_turns":1,"result":"%s","session_id":"T-fake-amp","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100},"permission_denials":[]}\n' "$text"
"#,
    )
    .unwrap();
    let mut perms = std::fs::metadata(&fake_amp).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_amp, perms).unwrap();

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(&mut writer, 2, "model/list", json!({})).await;
    let models = assert_response(&mut lines, 2).await;
    let model_ids = models["result"]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(model_ids, vec!["low", "medium", "high", "ultra"]);
    for model in models["result"]["data"].as_array().unwrap() {
        assert_eq!(model["defaultReasoningEffort"], "none");
        assert!(
            model["supportedReasoningEfforts"]
                .as_array()
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(models["result"]["data"][1]["isDefault"], true);

    send_request(
        &mut writer,
        20,
        "model/list",
        json!({"includeHidden": true}),
    )
    .await;
    let hidden_models = assert_response(&mut lines, 20).await;
    let hidden_model_ids = hidden_models["result"]["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|model| model["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(hidden_model_ids, vec!["low", "medium", "high", "ultra"]);

    send_request(
        &mut writer,
        3,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy()
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 3).await;
    assert_eq!(thread_start["result"]["model"], "medium");
    assert!(thread_start["result"]["reasoningEffort"].is_null());
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        4,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "hello amp", "text_elements": []}],
            "effort": "high"
        }),
    )
    .await;

    let mut saw_turn_response = false;
    let mut saw_turn_completed = false;
    let mut saw_agent_delta = false;
    for _ in 0..32 {
        let frame = read_frame(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(4) {
            saw_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta") {
            assert_eq!(frame["params"]["delta"], "hello from fake amp");
            saw_agent_delta = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_turn_completed = true;
        }
        if saw_turn_response && saw_agent_delta && saw_turn_completed {
            break;
        }
    }
    assert!(saw_turn_response, "missing turn/start response");
    assert!(saw_agent_delta, "missing assistant text delta");
    assert!(saw_turn_completed, "missing turn/completed");

    send_request(
        &mut writer,
        5,
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": true
        }),
    )
    .await;
    let thread_read = assert_response(&mut lines, 5).await;
    assert_eq!(thread_read["result"]["thread"]["sessionId"], "T-fake-amp");
    assert_eq!(
        thread_read["result"]["thread"]["turns"][0]["items"][1]["text"],
        "hello from fake amp"
    );

    send_request(
        &mut writer,
        6,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "second amp", "text_elements": []}],
            "effort": "xhigh"
        }),
    )
    .await;

    let mut saw_second_turn_response = false;
    let mut saw_second_turn_completed = false;
    let mut saw_second_agent_delta = false;
    for _ in 0..32 {
        let frame = read_frame(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(6) {
            saw_second_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta")
            && frame["params"]["delta"] == "second from fake amp"
        {
            saw_second_agent_delta = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_second_turn_completed = true;
        }
        if saw_second_turn_response && saw_second_agent_delta && saw_second_turn_completed {
            break;
        }
    }
    assert!(
        saw_second_turn_response,
        "missing second turn/start response"
    );
    assert!(
        saw_second_agent_delta,
        "missing second assistant text delta"
    );
    assert!(saw_second_turn_completed, "missing second turn/completed");

    send_request(
        &mut writer,
        7,
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": true
        }),
    )
    .await;
    let second_thread_read = assert_response(&mut lines, 7).await;
    assert_eq!(
        second_thread_read["result"]["thread"]["turns"][1]["items"][1]["text"],
        "second from fake amp"
    );
}

#[tokio::test]
async fn fake_amp_legacy_large_mode_normalizes_to_ultra() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-large.sh");
    std::fs::write(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
case " $* " in
  *" --mode ultra "*) ;;
  *) echo "missing --mode ultra in amp argv: $*" >&2; exit 7 ;;
esac
case " $* " in
  *" --effort "*) echo "unexpected --effort for large mode argv: $*" >&2; exit 8 ;;
esac
first=""
if IFS= read -r first; then
  :
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-fake-large","tools":[],"mcp_servers":[]}'
printf '{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"large ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}},"parent_tool_use_id":null,"session_id":"T-fake-large"}\n'
cat >/dev/null || true
printf '{"type":"result","subtype":"success","duration_ms":42,"is_error":false,"num_turns":1,"result":"large ok","session_id":"T-fake-large","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}}\n'
"#,
    )
    .unwrap();
    let mut perms = std::fs::metadata(&fake_amp).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_amp, perms).unwrap();

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "large"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    assert_eq!(thread_start["result"]["model"], "ultra");
    assert!(thread_start["result"]["reasoningEffort"].is_null());
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "hello large", "text_elements": []}]
        }),
    )
    .await;

    let mut saw_turn_response = false;
    let mut saw_turn_completed = false;
    let mut saw_agent_delta = false;
    for _ in 0..32 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(3) {
            saw_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta") {
            assert_eq!(frame["params"]["delta"], "large ok");
            saw_agent_delta = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_turn_completed = true;
        }
        if saw_turn_response && saw_agent_delta && saw_turn_completed {
            break;
        }
    }
    assert!(saw_turn_response, "missing turn/start response");
    assert!(saw_agent_delta, "missing assistant text delta");
    assert!(saw_turn_completed, "missing turn/completed");
}

#[tokio::test]
async fn fake_amp_legacy_mode_and_stale_effort_are_normalized_and_ignored() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-rush.sh");
    std::fs::write(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
case " $* " in
  *" --mode low "*) ;;
  *) echo "missing --mode low in amp argv: $*" >&2; exit 7 ;;
esac
case " $* " in
  *" --effort "*) echo "unexpected --effort for rush mode argv: $*" >&2; exit 8 ;;
esac
first=""
if IFS= read -r first; then
  :
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-fake-rush","tools":[],"mcp_servers":[]}'
printf '{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"rush ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}},"parent_tool_use_id":null,"session_id":"T-fake-rush"}\n'
cat >/dev/null || true
printf '{"type":"result","subtype":"success","duration_ms":42,"is_error":false,"num_turns":1,"result":"rush ok","session_id":"T-fake-rush","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}}\n'
"#,
    )
    .unwrap();
    let mut perms = std::fs::metadata(&fake_amp).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_amp, perms).unwrap();

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "medium"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    assert!(thread_start["result"]["reasoningEffort"].is_null());
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "thread/resume",
        json!({
            "threadId": thread_id,
            "model": "rush"
        }),
    )
    .await;
    let thread_resume = assert_response(&mut lines, 3).await;
    assert_eq!(thread_resume["result"]["model"], "low");
    assert!(thread_resume["result"]["reasoningEffort"].is_null());

    send_request(
        &mut writer,
        4,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "hello rush", "text_elements": []}]
        }),
    )
    .await;

    let mut saw_turn_response = false;
    let mut saw_turn_completed = false;
    let mut saw_agent_delta = false;
    for _ in 0..32 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(4) {
            saw_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("item/agentMessage/delta") {
            assert_eq!(frame["params"]["delta"], "rush ok");
            saw_agent_delta = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_turn_completed = true;
        }
        if saw_turn_response && saw_agent_delta && saw_turn_completed {
            break;
        }
    }
    assert!(saw_turn_response, "missing turn/start response");
    assert!(saw_agent_delta, "missing assistant text delta");
    assert!(saw_turn_completed, "missing turn/completed");
}

#[tokio::test]
async fn fake_amp_exit_without_result_fails_turn() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-exits.sh");
    std::fs::write(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
case " $* " in
  *" --mode high "*) ;;
  *) echo "missing --mode high in amp argv: $*" >&2; exit 7 ;;
esac
case " $* " in
  *" --effort "*) echo "unexpected --effort in high mode argv: $*" >&2; exit 8 ;;
esac
first=""
if IFS= read -r first; then
  :
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-fake-amp","tools":[],"mcp_servers":[]}'
exit 12
"#,
    )
    .unwrap();
    let mut perms = std::fs::metadata(&fake_amp).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&fake_amp, perms).unwrap();

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "high"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "hello amp", "text_elements": []}],
            "effort": "high"
        }),
    )
    .await;

    let mut saw_turn_response = false;
    let mut saw_turn_completed = false;
    for _ in 0..32 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(3) {
            saw_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "failed");
            assert!(
                frame["params"]["turn"]["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("amp stdout closed before result"),
                "unexpected error frame: {frame}"
            );
            saw_turn_completed = true;
        }
        if saw_turn_response && saw_turn_completed {
            break;
        }
    }
    assert!(saw_turn_response, "missing turn/start response");
    assert!(saw_turn_completed, "missing failed turn/completed");

    send_request(
        &mut writer,
        4,
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": true
        }),
    )
    .await;
    let thread_read = assert_response(&mut lines, 4).await;
    assert_eq!(
        thread_read["result"]["thread"]["turns"][0]["status"],
        "failed"
    );
}

#[tokio::test]
async fn fake_amp_on_request_does_not_force_dangerously_allow_all() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-policy.sh");
    write_executable(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
case " $* " in
  *" --dangerously-allow-all "*) echo "unexpected --dangerously-allow-all in argv: $*" >&2; exit 7 ;;
esac
first=""
if IFS= read -r first; then
  :
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-policy","tools":[],"mcp_servers":[]}'
printf '{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"policy ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}},"parent_tool_use_id":null,"session_id":"T-policy"}\n'
cat >/dev/null || true
printf '{"type":"result","subtype":"success","duration_ms":42,"is_error":false,"num_turns":1,"result":"policy ok","session_id":"T-policy","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}}\n'
"#,
    );

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "approvalPolicy": "on-request"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    assert_eq!(thread_start["result"]["approvalPolicy"], "on-request");
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "check policy", "text_elements": []}]
        }),
    )
    .await;

    let mut saw_turn_response = false;
    let mut saw_turn_completed = false;
    for _ in 0..32 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(3) {
            assert!(frame.get("error").is_none(), "response error: {frame}");
            saw_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_turn_completed = true;
        }
        if saw_turn_response && saw_turn_completed {
            break;
        }
    }
    assert!(saw_turn_response, "missing turn/start response");
    assert!(saw_turn_completed, "missing turn/completed");
}

#[tokio::test]
async fn fake_amp_buffers_init_emitted_before_input_is_written() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-early-init.sh");
    write_executable(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-early-init","tools":[],"mcp_servers":[]}'
sleep 0.1
first=""
if IFS= read -r first; then
  :
fi
printf '{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"early ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}},"parent_tool_use_id":null,"session_id":"T-early-init"}\n'
cat >/dev/null || true
printf '{"type":"result","subtype":"success","duration_ms":42,"is_error":false,"num_turns":1,"result":"early ok","session_id":"T-early-init","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}}\n'
"#,
    );

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "medium"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "hello early", "text_elements": []}]
        }),
    )
    .await;

    let mut saw_turn_response = false;
    let mut saw_turn_completed = false;
    for _ in 0..32 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(3) {
            assert!(frame.get("error").is_none(), "response error: {frame}");
            saw_turn_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_turn_completed = true;
        }
        if saw_turn_response && saw_turn_completed {
            break;
        }
    }
    assert!(saw_turn_response, "missing turn/start response");
    assert!(saw_turn_completed, "missing turn/completed");

    send_request(
        &mut writer,
        4,
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": false
        }),
    )
    .await;
    let thread_read = assert_response(&mut lines, 4).await;
    assert_eq!(thread_read["result"]["thread"]["sessionId"], "T-early-init");
}

#[tokio::test]
async fn fake_amp_initial_write_failure_clears_active_turn() {
    let temp = TempDir::new().unwrap();
    let launcher = Arc::new(BrokenPipeLauncher::default());
    let bridge = AmpBridge::builder()
        .agent_bin("fake-amp")
        .launcher(launcher.clone())
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "medium"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    for id in [3, 4] {
        send_request(
            &mut writer,
            id,
            "turn/start",
            json!({
                "threadId": thread_id,
                "input": [{"type": "text", "text": format!("broken {id}"), "text_elements": []}]
            }),
        )
        .await;
        let error = assert_error_response(&mut lines, id).await;
        let message = error["error"]["message"].as_str().unwrap_or_default();
        assert!(
            !message.contains("already has an active amp turn"),
            "active turn leaked after failed write: {error}"
        );
    }

    assert_eq!(launcher.launches.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn fake_amp_interrupt_wrong_turn_id_keeps_active_turn() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-sleep.sh");
    write_executable(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
first=""
if IFS= read -r first; then
  :
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-sleep","tools":[],"mcp_servers":[]}'
sleep 30
"#,
    );

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "medium"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "sleep", "text_elements": []}]
        }),
    )
    .await;
    let turn_start = assert_response(&mut lines, 3).await;
    let turn_id = turn_start["result"]["turn"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        4,
        "turn/interrupt",
        json!({
            "threadId": thread_id,
            "turnId": "stale-turn-id"
        }),
    )
    .await;
    let stale = assert_error_response(&mut lines, 4).await;
    assert!(
        stale["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("does not match active turn"),
        "unexpected stale interrupt response: {stale}"
    );

    send_request(
        &mut writer,
        5,
        "turn/interrupt",
        json!({
            "threadId": thread_id,
            "turnId": turn_id
        }),
    )
    .await;
    let mut saw_interrupt_response = false;
    let mut saw_turn_completed = false;
    for _ in 0..16 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(5) {
            assert!(frame.get("error").is_none(), "response error: {frame}");
            saw_interrupt_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "interrupted");
            saw_turn_completed = true;
        }
        if saw_interrupt_response && saw_turn_completed {
            break;
        }
    }
    assert!(saw_interrupt_response, "missing correct interrupt response");
    assert!(saw_turn_completed, "missing interrupted turn/completed");
}

#[tokio::test]
async fn fake_amp_concurrent_turn_start_reserves_before_launch() {
    let temp = TempDir::new().unwrap();
    let fake_amp = temp.path().join("fake-amp-gated.sh");
    write_executable(
        &fake_amp,
        r#"#!/usr/bin/env bash
set -euo pipefail
first=""
if IFS= read -r first; then
  :
fi
echo '{"type":"system","subtype":"init","cwd":"/tmp/fake","session_id":"T-gated","tools":[],"mcp_servers":[]}'
printf '{"type":"assistant","message":{"type":"message","role":"assistant","content":[{"type":"text","text":"gated ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}},"parent_tool_use_id":null,"session_id":"T-gated"}\n'
cat >/dev/null || true
printf '{"type":"result","subtype":"success","duration_ms":42,"is_error":false,"num_turns":1,"result":"gated ok","session_id":"T-gated","usage":{"input_tokens":1,"output_tokens":2,"max_tokens":100}}\n'
"#,
    );
    let launcher = Arc::new(GatedLauncher::new());

    let bridge = AmpBridge::builder()
        .agent_bin(&fake_amp)
        .launcher(launcher.clone())
        .codex_home(temp.path().join("codex-home"))
        .build()
        .await
        .unwrap();

    let (client, server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        serve_stream(bridge, server).await.unwrap();
    });

    let (reader, mut writer) = tokio::io::split(client);
    let mut lines = BufReader::new(reader).lines();

    send_request(
        &mut writer,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "test", "version": "0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await;
    assert_response(&mut lines, 1).await;

    send_request(
        &mut writer,
        2,
        "thread/start",
        json!({
            "cwd": temp.path().to_string_lossy(),
            "model": "medium"
        }),
    )
    .await;
    let thread_start = assert_response(&mut lines, 2).await;
    let thread_id = thread_start["result"]["thread"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    send_request(
        &mut writer,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "first", "text_elements": []}]
        }),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), launcher.started.notified())
        .await
        .expect("timed out waiting for first launch to start");

    send_request(
        &mut writer,
        4,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "second", "text_elements": []}]
        }),
    )
    .await;
    let second = assert_error_response(&mut lines, 4).await;
    assert!(
        second["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("already has an active amp turn"),
        "unexpected concurrent start response: {second}"
    );
    assert_eq!(launcher.spec_count(), 1);

    launcher.release.notify_waiters();

    let mut saw_first_response = false;
    let mut saw_turn_completed = false;
    for _ in 0..32 {
        let frame = read_frame_timeout(&mut lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(3) {
            assert!(frame.get("error").is_none(), "response error: {frame}");
            saw_first_response = true;
        }
        if frame.get("method").and_then(Value::as_str) == Some("turn/completed") {
            assert_eq!(frame["params"]["turn"]["status"], "completed");
            saw_turn_completed = true;
        }
        if saw_first_response && saw_turn_completed {
            break;
        }
    }
    assert!(saw_first_response, "missing first turn/start response");
    assert!(saw_turn_completed, "missing first turn/completed");
}

fn write_executable(path: &std::path::Path, contents: &str) {
    std::fs::write(path, contents).unwrap();
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).unwrap();
}

#[derive(Default)]
struct BrokenPipeLauncher {
    launches: AtomicUsize,
}

impl ProcessLauncher for BrokenPipeLauncher {
    fn launch(&self, _spec: ProcessSpec) -> BoxFuture<'_, std::io::Result<Box<dyn ChildProcess>>> {
        self.launches.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Box::new(BrokenPipeChild::new()) as Box<dyn ChildProcess>) })
    }
}

struct BrokenPipeChild {
    stdin: Mutex<Option<ChildStdin>>,
    stdout: Mutex<Option<ChildStdout>>,
    stderr: Mutex<Option<ChildStderr>>,
}

impl BrokenPipeChild {
    fn new() -> Self {
        Self {
            stdin: Mutex::new(Some(Box::new(BrokenWriter))),
            stdout: Mutex::new(Some(Box::new(EmptyReader))),
            stderr: Mutex::new(Some(Box::new(EmptyReader))),
        }
    }
}

impl ChildProcess for BrokenPipeChild {
    fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.stdin.lock().unwrap().take()
    }

    fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.stdout.lock().unwrap().take()
    }

    fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.stderr.lock().unwrap().take()
    }

    fn id(&self) -> Option<u32> {
        None
    }

    fn wait(&mut self) -> BoxFuture<'_, std::io::Result<ExitStatus>> {
        Box::pin(async { Ok(ExitStatus::from_raw(0)) })
    }

    fn kill(&mut self) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

struct BrokenWriter;

impl AsyncWrite for BrokenWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "test broken stdin",
        )))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct EmptyReader;

impl AsyncRead for EmptyReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

struct GatedLauncher {
    started: Arc<Notify>,
    release: Arc<Notify>,
    specs: Arc<Mutex<Vec<Vec<String>>>>,
}

impl GatedLauncher {
    fn new() -> Self {
        Self {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            specs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn spec_count(&self) -> usize {
        self.specs.lock().unwrap().len()
    }
}

impl ProcessLauncher for GatedLauncher {
    fn launch(&self, spec: ProcessSpec) -> BoxFuture<'_, std::io::Result<Box<dyn ChildProcess>>> {
        let started = Arc::clone(&self.started);
        let release = Arc::clone(&self.release);
        let specs = Arc::clone(&self.specs);
        Box::pin(async move {
            specs.lock().unwrap().push(
                spec.args
                    .iter()
                    .map(|arg| arg.to_string_lossy().into_owned())
                    .collect(),
            );
            started.notify_one();
            release.notified().await;
            let local = LocalLauncher;
            local.launch(spec).await
        })
    }
}

async fn send_request(
    writer: &mut tokio::io::WriteHalf<DuplexStream>,
    id: i64,
    method: &str,
    params: Value,
) {
    let frame = json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    });
    writer
        .write_all(serde_json::to_string(&frame).unwrap().as_bytes())
        .await
        .unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

async fn assert_response<R>(lines: &mut tokio::io::Lines<BufReader<R>>, id: i64) -> Value
where
    R: tokio::io::AsyncRead + Unpin,
{
    for _ in 0..16 {
        let frame = read_frame(lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(id) {
            assert!(frame.get("error").is_none(), "response error: {frame}");
            return frame;
        }
    }
    panic!("missing response id {id}");
}

async fn assert_error_response<R>(lines: &mut tokio::io::Lines<BufReader<R>>, id: i64) -> Value
where
    R: tokio::io::AsyncRead + Unpin,
{
    for _ in 0..16 {
        let frame = read_frame_timeout(lines).await;
        if frame.get("id").and_then(Value::as_i64) == Some(id) {
            assert!(
                frame.get("error").is_some(),
                "response was not an error: {frame}"
            );
            return frame;
        }
    }
    panic!("missing error response id {id}");
}

async fn read_frame<R>(lines: &mut tokio::io::Lines<BufReader<R>>) -> Value
where
    R: tokio::io::AsyncRead + Unpin,
{
    let line = lines.next_line().await.unwrap().unwrap();
    serde_json::from_str(&line).unwrap()
}

async fn read_frame_timeout<R>(lines: &mut tokio::io::Lines<BufReader<R>>) -> Value
where
    R: tokio::io::AsyncRead + Unpin,
{
    tokio::time::timeout(Duration::from_secs(3), read_frame(lines))
        .await
        .expect("timed out waiting for JSON-RPC frame")
}
