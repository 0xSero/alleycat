use base64::Engine;
use reqwest::{Method, Response};
use serde_json::{Value, json};

#[derive(Clone)]
pub struct OpencodeClient {
    http: reqwest::Client,
    base_url: String,
    auth_token: String,
}

impl OpencodeClient {
    pub fn new(base_url: String, auth_token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
            auth_token,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn get(&self, path: &str) -> anyhow::Result<Value> {
        self.request(Method::GET, path, None).await
    }

    pub async fn post(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        self.request(Method::POST, path, Some(body)).await
    }

    pub async fn patch(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        self.request(Method::PATCH, path, Some(body)).await
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<Value> {
        self.request(Method::DELETE, path, None).await
    }

    pub async fn raw_get(&self, path: &str) -> anyhow::Result<Response> {
        let url = self.url(path);
        let resp = self.http.get(url).send().await?;
        Ok(resp.error_for_status()?)
    }

    async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> anyhow::Result<Value> {
        let mut req = self.http.request(method, self.url(path));
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req.send().await?.error_for_status()?;
        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        Ok(resp.json().await?)
    }

    fn url(&self, path: &str) -> String {
        let sep = if path.contains('?') { '&' } else { '?' };
        if self.auth_token.is_empty() {
            format!("{}{}", self.base_url, path)
        } else {
            format!(
                "{}{}{}auth_token={}",
                self.base_url, path, sep, self.auth_token
            )
        }
    }

    /// `ws://…/pty/{id}/connect[?auth_token=…]`. Exposed so `pty.rs` can own
    /// a long-lived websocket per process for the full `command/exec`
    /// lifetime.
    pub fn pty_connect_url(&self, pty_id: &str) -> String {
        self.url(&format!("/pty/{pty_id}/connect"))
            .replacen("http://", "ws://", 1)
            .replacen("https://", "wss://", 1)
    }

    pub async fn pty_create(&self, body: Value) -> anyhow::Result<Value> {
        self.post("/pty", body).await
    }

    pub async fn pty_remove(&self, pty_id: &str) -> anyhow::Result<()> {
        self.delete(&format!("/pty/{pty_id}")).await?;
        Ok(())
    }

    pub async fn pty_resize(&self, pty_id: &str, rows: u32, cols: u32) -> anyhow::Result<()> {
        self.put(
            &format!("/pty/{pty_id}"),
            json!({"size":{"rows":rows,"cols":cols}}),
        )
        .await?;
        Ok(())
    }

    pub async fn put(&self, path: &str, body: Value) -> anyhow::Result<Value> {
        self.request(Method::PUT, path, Some(body)).await
    }

    pub async fn create_session(
        &self,
        title: Option<String>,
        permission: Option<Value>,
    ) -> anyhow::Result<Value> {
        let mut body = serde_json::Map::new();
        if let Some(title) = title {
            body.insert("title".to_string(), json!(title));
        }
        if let Some(permission) = permission {
            body.insert("permission".to_string(), permission);
        }
        self.post("/session", Value::Object(body)).await
    }

    pub async fn summarize_session(
        &self,
        session_id: &str,
        provider_id: &str,
        model_id: &str,
    ) -> anyhow::Result<Value> {
        self.post(
            &format!("/session/{session_id}/summarize"),
            json!({"providerID": provider_id, "modelID": model_id, "auto": false}),
        )
        .await
    }

    pub async fn revert_session(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> anyhow::Result<Value> {
        self.post(
            &format!("/session/{session_id}/revert"),
            json!({"messageID": message_id}),
        )
        .await
    }

    pub async fn fork_session(
        &self,
        session_id: &str,
        message_id: Option<&str>,
    ) -> anyhow::Result<Value> {
        let body = match message_id {
            Some(id) => json!({"messageID": id}),
            None => json!({}),
        };
        self.post(&format!("/session/{session_id}/fork"), body)
            .await
    }

    pub async fn list_messages(&self, session_id: &str) -> anyhow::Result<Value> {
        self.get(&format!("/session/{session_id}/message")).await
    }

    /// Fetch a bounded page of session messages using opencode's native
    /// message pagination (`GET /session/:id/message?limit=&before=`).
    ///
    /// The legacy endpoint deliberately returns only the message array; it
    /// does not expose the continuation cursor that `MessageV2.page` creates
    /// internally. `has_more` is therefore conservative when a full page is
    /// returned. Callers construct the next `before` cursor from the oldest
    /// message boundary they actually emit.
    pub async fn list_messages_page(
        &self,
        session_id: &str,
        limit: Option<u32>,
        before: Option<&str>,
    ) -> anyhow::Result<(Vec<Value>, bool)> {
        let mut query = Vec::new();
        if let Some(limit) = limit {
            query.push(format!("limit={limit}"));
        }
        if let Some(before) = before.filter(|s| !s.is_empty()) {
            query.push(format!("before={before}"));
        }
        let path = if query.is_empty() {
            format!("/session/{session_id}/message")
        } else {
            format!("/session/{session_id}/message?{}", query.join("&"))
        };
        let resp = self.raw_get(&path).await?;
        let body: Value = resp.json().await?;
        let messages = body.as_array().cloned().unwrap_or_default();
        let has_more = limit.is_some_and(|limit| messages.len() >= limit as usize);
        Ok((messages, has_more))
    }

    /// `POST /session/:id/prompt_async` — opencode accepts the prompt and
    /// returns 204 immediately; the actual model work is driven over SSE.
    pub async fn prompt_async(&self, session_id: &str, body: Value) -> anyhow::Result<()> {
        self.post(&format!("/session/{session_id}/prompt_async"), body)
            .await?;
        Ok(())
    }

    /// `POST /permission/:requestID/reply` — settle an `asked` permission
    /// prompt. `reply` is one of `"once" | "always" | "reject"` per
    /// `~/dev/opencode/packages/opencode/src/permission/index.ts:Reply`.
    pub async fn permission_reply(
        &self,
        request_id: &str,
        reply: &str,
        message: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut body = json!({"reply": reply});
        if let Some(message) = message {
            body["message"] = json!(message);
        }
        self.post(&format!("/permission/{request_id}/reply"), body)
            .await?;
        Ok(())
    }

    /// `POST /question/:requestID/reply` — settle an `asked` question. `answers`
    /// is `Array<Array<string>>` ordered to match the question array in the
    /// original `Question.Request` (see
    /// `~/dev/opencode/packages/opencode/src/question/index.ts:Reply`).
    pub async fn question_reply(&self, request_id: &str, answers: Value) -> anyhow::Result<()> {
        self.post(
            &format!("/question/{request_id}/reply"),
            json!({"answers": answers}),
        )
        .await?;
        Ok(())
    }
}

/// Encode the cursor accepted by opencode's legacy `before` query. This is
/// the same base64url JSON shape used by `MessageV2.cursor.encode`.
pub(crate) fn message_before_cursor(message: &Value) -> anyhow::Result<String> {
    let id = message
        .pointer("/info/id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("message is missing info.id"))?;
    let time = message
        .pointer("/info/time/created")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("message is missing info.time.created"))?;
    let encoded = serde_json::to_vec(&json!({"id": id, "time": time}))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(encoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_before_cursor_matches_opencode_shape() {
        let cursor = message_before_cursor(&json!({
            "info": {"id": "msg_123", "time": {"created": 456}}
        }))
        .expect("cursor");
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(cursor)
            .expect("base64url cursor");
        let value: Value = serde_json::from_slice(&decoded).expect("cursor json");
        assert_eq!(value, json!({"id": "msg_123", "time": 456}));
    }
}
