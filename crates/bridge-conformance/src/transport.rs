//! Shared JSON-RPC client used by every conformance target.
//!
//! Every target produces a pair of (writer, reader) trait objects from its
//! transport (usually stdio pipes, with backend-specific socket transports
//! where needed). The harness drives them through this client, which handles
//! id allocation, request/response correlation, and notification capture.

use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::time::timeout;

/// Boxed reader. We read line-delimited JSON; a `BufReader` wrapper is
/// the caller's responsibility.
pub type FrameReader = BufReader<Box<dyn tokio::io::AsyncRead + Send + Unpin>>;
pub type FrameWriter = Box<dyn tokio::io::AsyncWrite + Send + Unpin>;

/// JSON-RPC client multiplexed over a single line-delimited stream.
///
/// Not thread-safe across requests — the harness drives one op at a time.
pub struct JsonRpcClient {
    writer: FrameWriter,
    reader: FrameReader,
    next_id: AtomicI64,
    /// Responses for another in-flight request that arrived while we were
    /// waiting for a different id.
    pending_responses: Vec<Value>,
}

impl JsonRpcClient {
    pub fn new(reader: FrameReader, writer: FrameWriter) -> Self {
        Self {
            reader,
            writer,
            next_id: AtomicI64::new(1),
            pending_responses: Vec::new(),
        }
    }

    /// Send a request, wait until a frame with the matching id arrives, and
    /// return both the response and any notifications that arrived in between.
    pub async fn request(
        &mut self,
        method: &str,
        params: Value,
        deadline: Duration,
    ) -> Result<RequestOutcome> {
        let id = self.send_request(method, params).await?;
        self.wait_for_response(method, id, deadline).await
    }

    /// Send a JSON-RPC request and return its id without waiting for the
    /// response. Used by scenario steps that intentionally keep one request
    /// active while driving a companion control method.
    pub async fn send_request(&mut self, method: &str, params: Value) -> Result<i64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        write_frame(&mut self.writer, &frame).await?;
        Ok(id)
    }

    /// Wait until the response for a previously sent request arrives,
    /// returning every notification seen while waiting.
    pub async fn wait_for_response(
        &mut self,
        method: &str,
        id: i64,
        deadline: Duration,
    ) -> Result<RequestOutcome> {
        let mut notifications = Vec::new();
        let started = std::time::Instant::now();
        loop {
            if let Some(value) = self.take_pending_response(id) {
                return Ok(RequestOutcome {
                    response: value,
                    notifications,
                });
            }
            let remaining = deadline.checked_sub(started.elapsed()).unwrap_or_default();
            if remaining.is_zero() {
                bail!("timed out waiting for response id={id} method={method}");
            }
            let value = self.read_wire_one(remaining).await?;
            if value.get("id").and_then(Value::as_i64) == Some(id) {
                return Ok(RequestOutcome {
                    response: value,
                    notifications,
                });
            }
            if value.get("id").is_some() && value.get("method").is_none() {
                self.pending_responses.push(value);
                continue;
            }
            // It's a notification or a server-initiated request. We don't
            // currently respond to server-initiated requests; record both
            // shapes as notifications for diffing purposes (the dispatcher
            // can disambiguate via the presence of `id`+`method`).
            notifications.push(value);
        }
    }

    /// Send a notification (no id, no response).
    pub async fn notify(&mut self, method: &str, params: Option<Value>) -> Result<()> {
        let mut frame = json!({ "jsonrpc": "2.0", "method": method });
        if let Some(p) = params {
            frame["params"] = p;
        }
        write_frame(&mut self.writer, &frame).await
    }

    /// Continue reading frames until either we see a notification with one of
    /// the listed methods (`stop_methods`) or `deadline` elapses. Returns all
    /// notifications collected including the terminator (if any).
    pub async fn drain_notifications_until(
        &mut self,
        stop_methods: &[&str],
        deadline: Duration,
    ) -> Result<DrainOutcome> {
        let mut collected = Vec::new();
        let started = std::time::Instant::now();
        loop {
            let remaining = deadline.checked_sub(started.elapsed()).unwrap_or_default();
            if remaining.is_zero() {
                return Ok(DrainOutcome {
                    notifications: collected,
                    terminated_by: None,
                });
            }
            let value = match self.read_wire_one(remaining).await {
                Ok(v) => v,
                Err(err) => {
                    // Timeout or EOF — return what we have.
                    tracing::debug!(?err, "drain ended");
                    return Ok(DrainOutcome {
                        notifications: collected,
                        terminated_by: None,
                    });
                }
            };
            if let Some(method) = value.get("method").and_then(Value::as_str) {
                let matched = stop_methods.contains(&method);
                let owned_method = method.to_string();
                collected.push(value);
                if matched {
                    return Ok(DrainOutcome {
                        notifications: collected,
                        terminated_by: Some(owned_method),
                    });
                }
            } else {
                // Stray response with no matching outstanding request — keep
                // it in the buffer so a future `request()` can correlate.
                self.pending_responses.push(value);
            }
        }
    }

    fn take_pending_response(&mut self, id: i64) -> Option<Value> {
        let pos = self
            .pending_responses
            .iter()
            .position(|value| value.get("id").and_then(Value::as_i64) == Some(id))?;
        Some(self.pending_responses.remove(pos))
    }

    async fn read_wire_one(&mut self, deadline: Duration) -> Result<Value> {
        let mut line = String::new();
        let n = timeout(deadline, self.reader.read_line(&mut line))
            .await
            .map_err(|_| anyhow!("read timeout after {deadline:?}"))?
            .context("reading transport line")?;
        if n == 0 {
            bail!("transport closed");
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            // Empty keepalive line; recurse via Box::pin to avoid async fn
            // recursion concerns on stable.
            return Box::pin(self.read_wire_one(deadline)).await;
        }
        serde_json::from_str(trimmed).with_context(|| format!("parsing frame: {trimmed}"))
    }

    /// Drain notifications that arrive within `quiet_window` of the last one
    /// (i.e., wait for the bridge to fall idle before returning). Used after a
    /// request to catch async notifications codex/bridges emit *after* the
    /// JSON-RPC response — without this, those notifications get attributed
    /// to the next step's drain window.
    pub async fn drain_idle(&mut self, quiet_window: Duration) -> Vec<Value> {
        let mut collected = Vec::new();
        loop {
            match self.read_wire_one(quiet_window).await {
                Ok(value) => {
                    if value.get("method").is_some() {
                        collected.push(value);
                    } else {
                        // Stray response — buffer for next request().
                        self.pending_responses.push(value);
                    }
                }
                Err(_) => return collected, // timeout = idle
            }
        }
    }

    pub async fn shutdown(mut self) {
        // Best-effort flush; ignore failures — the transport may already be
        // half-closed by the time we get here.
        let _ = self.writer.flush().await;
        let _ = self.writer.shutdown().await;
    }
}

#[derive(Debug)]
pub struct RequestOutcome {
    pub response: Value,
    pub notifications: Vec<Value>,
}

#[derive(Debug)]
pub struct DrainOutcome {
    pub notifications: Vec<Value>,
    /// `Some(method)` if a notification matching `stop_methods` arrived;
    /// `None` if the drain timed out or the stream closed first.
    pub terminated_by: Option<String>,
}

async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(value).context("serialize JSON-RPC frame")?;
    bytes.push(b'\n');
    writer
        .write_all(&bytes)
        .await
        .context("writing JSON-RPC frame")?;
    writer.flush().await.context("flushing JSON-RPC frame")?;
    Ok(())
}

/// Helper to wrap an arbitrary `AsyncRead` into our boxed reader type.
pub fn boxed_reader<R>(reader: R) -> FrameReader
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
{
    BufReader::new(Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>)
}

/// Helper to wrap an arbitrary `AsyncWrite` into our boxed writer type.
pub fn boxed_writer<W>(writer: W) -> FrameWriter
where
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    Box::new(writer) as FrameWriter
}

// Suppress unused-import warning for Reader trait when target features get
// trimmed at compile time.
#[allow(dead_code)]
fn _silence_unused(_r: &dyn AsyncBufRead) {}
