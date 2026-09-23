use std::path::Path;

use base64::Engine;
use serde_json::{Value, json};

pub fn codex_input_to_parts(input: &[Value]) -> Vec<Value> {
    let mut parts = Vec::new();
    for item in input {
        match item.get("type").and_then(Value::as_str) {
            Some("text") => {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    parts.push(json!({"type":"text","text":text}));
                }
            }
            Some("image") => {
                if let Some(url) = item.get("url").and_then(Value::as_str) {
                    parts.push(json!({"type":"file","mime":"image/*","url":url}));
                }
            }
            Some("localImage") => {
                if let Some(path) = item.get("path").and_then(Value::as_str) {
                    parts.push(local_image_to_part(path));
                }
            }
            Some("skill") => {
                if let Some(name) = item.get("name").and_then(Value::as_str) {
                    parts.push(json!({"type":"agent","name":name}));
                }
            }
            Some("mention") => {
                let name = item.get("name").and_then(Value::as_str).unwrap_or("");
                let path = item.get("path").and_then(Value::as_str);
                parts.push(json!({"type":"text","text":format!("@{name}")}));
                if let Some(path) = path
                    && let Some(file_part) = mention_file_part(path)
                {
                    parts.push(file_part);
                }
            }
            _ => {
                if let Some(text) = item.as_str() {
                    parts.push(json!({"type":"text","text":text}));
                }
            }
        }
    }
    if parts.is_empty() {
        tracing::debug!("codex input produced no parts; emitting empty text fallback");
        parts.push(json!({"type":"text","text":""}));
    }
    parts
}

fn local_image_to_part(path: &str) -> Value {
    match std::fs::read(path) {
        Ok(bytes) => {
            let mime = mime_for_path(path);
            let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
            let filename = Path::new(path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("image");
            json!({
                "type": "file",
                "mime": mime,
                "filename": filename,
                "url": format!("data:{mime};base64,{b64}")
            })
        }
        Err(err) => {
            tracing::warn!(
                ?err,
                path,
                "failed to read local image; falling back to text"
            );
            json!({"type":"text","text":format!("[local image unavailable: {path}]")})
        }
    }
}

fn mention_file_part(path: &str) -> Option<Value> {
    let bytes = std::fs::read(path).ok()?;
    let mime = mime_for_path(path);
    let filename = Path::new(path)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("file");
    let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Some(json!({
        "type": "file",
        "mime": mime,
        "filename": filename,
        "url": format!("data:{mime};base64,{b64}")
    }))
}

fn mime_for_path(path: &str) -> &'static str {
    let ext = Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        "heic" => "image/heic",
        "tif" | "tiff" => "image/tiff",
        "pdf" => "application/pdf",
        "txt" | "log" | "md" => "text/plain",
        "json" => "application/json",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" | "cjs" => "text/javascript",
        "ts" | "tsx" => "application/typescript",
        "rs" => "text/x-rust",
        "py" => "text/x-python",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn local_image_inlines_base64_data_url() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pic.png");
        let bytes: &[u8] = &[0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
        std::fs::File::create(&path)
            .unwrap()
            .write_all(bytes)
            .unwrap();
        let input = vec![json!({"type":"localImage","path":path.to_str().unwrap()})];
        let parts = codex_input_to_parts(&input);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "file");
        assert_eq!(parts[0]["mime"], "image/png");
        let url = parts[0]["url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,"), "url was {url}");
        assert_eq!(parts[0]["filename"], "pic.png");
    }

    #[test]
    fn local_image_missing_falls_back_to_text() {
        let input = vec![json!({"type":"localImage","path":"/nonexistent/path/x.png"})];
        let parts = codex_input_to_parts(&input);
        assert_eq!(parts[0]["type"], "text");
        assert!(
            parts[0]["text"]
                .as_str()
                .unwrap()
                .contains("/nonexistent/path/x.png")
        );
    }

    #[test]
    fn mention_emits_text_and_file_when_path_resolves() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("notes.md");
        std::fs::write(&path, b"hello").unwrap();
        let input = vec![json!({
            "type":"mention",
            "name":"notes",
            "path": path.to_str().unwrap()
        })];
        let parts = codex_input_to_parts(&input);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], "@notes");
        assert_eq!(parts[1]["type"], "file");
        assert_eq!(parts[1]["mime"], "text/plain");
    }

    #[test]
    fn mention_without_resolvable_path_keeps_text_only() {
        let input = vec![json!({
            "type":"mention",
            "name":"notes",
            "path":"/nope.md"
        })];
        let parts = codex_input_to_parts(&input);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], "@notes");
    }

    #[test]
    fn empty_input_falls_back_to_empty_text_part() {
        let parts = codex_input_to_parts(&[]);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], "");
    }
}
