//! Public settings metadata augments native user overrides, never invents values.
use super::{config_contains, sensitive};
use alleycat_codex_proto as p;
use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

// Officially linked by https://code.claude.com/docs/en/settings#edit-a-settings-file.
// Fetch only this fixed public source; configuration cannot nominate remote schemas.
const CLAUDE_SCHEMA: &str = "https://www.schemastore.org/claude-code-settings.json";
const SOURCE: &str = "Claude Code published JSON schema (https://json.schemastore.org/claude-code-settings.json; may lag the installed CLI)";
const DROID_SCHEMA: &str = "https://docs.factory.ai/droid-cli/settings.md";
const DROID_SOURCE: &str = "Factory's published user settings reference (https://docs.factory.ai/droid-cli/settings.md; unset override, not an effective default)";
type Parser = fn(&[u8]) -> Result<Value>;
const MAX_BYTES: usize = 1024 * 1024;
const MAX_FIELDS: usize = 4096;
const MAX_DEPTH: usize = 16;

#[derive(Default)]
struct SchemaCache {
    document: Option<Arc<Value>>,
    etag: Option<String>,
    refresh_after: Option<Instant>,
}
impl SchemaCache {
    async fn read(&mut self, url: &str, parser: Parser) -> Option<Arc<Value>> {
        if self.refresh_after.is_some_and(|next| Instant::now() < next) {
            return self.document.clone();
        }
        // Failed discovery must never hide configured values or replace a good cache.
        self.refresh_after = Some(Instant::now() + Duration::from_secs(60));
        if let Ok(fetched) = fetch(url, self.etag.as_deref(), parser).await {
            match fetched {
                Some((document, etag)) => {
                    self.document = Some(Arc::new(document));
                    self.etag = etag;
                }
                None if self.document.is_none() => return None,
                None => {}
            }
            self.refresh_after = Some(Instant::now() + Duration::from_secs(600));
        }
        self.document.clone()
    }
}
async fn fetch(
    url: &str,
    etag: Option<&str>,
    parser: Parser,
) -> Result<Option<(Value, Option<String>)>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .redirect(reqwest::redirect::Policy::limited(3))
        .build()?;
    let mut request = client.get(url);
    if let Some(etag) = etag {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    let mut response = request.send().await?;
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        return Ok(None);
    }
    response.error_for_status_ref()?;
    if response
        .content_length()
        .is_some_and(|n| n > MAX_BYTES as u64)
    {
        bail!("published schema is too large");
    }
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > MAX_BYTES {
            bail!("published schema is too large")
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Some((parser(&bytes)?, etag)))
}
fn parse_json_schema(bytes: &[u8]) -> Result<Value> {
    let document: Value = serde_json::from_slice(bytes)?;
    document["properties"]
        .as_object()
        .filter(|v| !v.is_empty())
        .context("published schema has no settings properties")?;
    Ok(document)
}

pub async fn append_claude_declared_settings(response: &mut p::ConfigReadResponse) {
    static CACHE: OnceLock<tokio::sync::Mutex<SchemaCache>> = OnceLock::new();
    let Some(schema) = CACHE
        .get_or_init(Default::default)
        .lock()
        .await
        .read(CLAUDE_SCHEMA, parse_json_schema)
        .await
    else {
        return;
    };
    append_schema(response, &schema, SOURCE);
}

pub async fn append_droid_declared_settings(response: &mut p::ConfigReadResponse) {
    static CACHE: OnceLock<tokio::sync::Mutex<SchemaCache>> = OnceLock::new();
    let Some(schema) = CACHE
        .get_or_init(Default::default)
        .lock()
        .await
        .read(DROID_SCHEMA, parse_droid_user_schema)
        .await
    else {
        return;
    };
    append_schema(response, &schema, DROID_SOURCE);
}
// The official LLM documentation index links this raw Markdown. Only typed
// Property tags in its personal-user sections are settings metadata. Tables,
// enterprise controls, Factory App preferences, and untyped sound labels are not.
fn parse_droid_user_schema(bytes: &[u8]) -> Result<Value> {
    let text = std::str::from_utf8(bytes)?;
    let (personal, _) = text
        .split_once("## Enterprise and org-level settings")
        .context("Factory user-settings scope boundary missing")?;
    let mut properties = serde_json::Map::new();
    for tail in personal.split("<Property").skip(1) {
        if !tail.starts_with(char::is_whitespace) {
            continue;
        }
        let Some((tag, _)) = tail.split_once('>') else {
            continue;
        };
        let attrs = attributes(tag);
        let Some(name) = attrs.get("name") else {
            continue;
        };
        let Some(kind) = attrs.get("type") else {
            continue;
        };
        if name.is_empty()
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'.' || c == b'_')
            || name.split('.').any(str::is_empty)
            || sensitive(name)
            || !matches!(
                kind.as_str(),
                "string" | "number" | "integer" | "boolean" | "array" | "object"
            )
        {
            continue;
        }
        // defaultValue can be prose ("Inherits", "Product default"); it is
        // deliberately never interpreted as a current value or schema default.
        properties.insert(name.clone(), json!({"type":kind}));
        if properties.len() >= MAX_FIELDS {
            break;
        }
    }
    if properties.is_empty() {
        bail!("Factory user settings metadata unavailable")
    }
    Ok(json!({"properties":properties}))
}
fn attributes(mut tag: &str) -> std::collections::HashMap<String, String> {
    let mut attrs = std::collections::HashMap::new();
    loop {
        tag = tag.trim_start();
        let length = tag.bytes().take_while(u8::is_ascii_alphanumeric).count();
        if length == 0 {
            break;
        }
        let key = &tag[..length];
        tag = tag[length..].trim_start();
        let Some(value) = tag.strip_prefix('=') else {
            break;
        };
        tag = value.trim_start();
        let Some(quote) = tag.chars().next().filter(|c| *c == '\'' || *c == '"') else {
            break;
        };
        tag = &tag[1..];
        let Some(end) = tag.find(quote) else { break };
        attrs.insert(key.to_owned(), tag[..end].to_owned());
        tag = &tag[end + 1..];
    }
    attrs
}

fn read_only_reason(node: &Value) -> Option<&'static str> {
    let description = node["description"]
        .as_str()
        .unwrap_or("")
        .to_ascii_lowercase();
    if description.contains("#global-config-settings")
        || description.contains("stored in ~/.claude.json")
    {
        return Some("This setting belongs to Claude's global config, not the user settings file");
    }
    let qualifier = description.split_once(')').map(|(v, _)| v).unwrap_or("");
    if (description.starts_with('(') && qualifier.contains("managed"))
        || [
            "only honored from managed",
            "honored only from managed",
            "honored only at the managed",
            "honored only from mdm",
        ]
        .iter()
        .any(|phrase| description.contains(phrase))
    {
        return Some(
            "The published schema marks this setting as managed-only; user settings cannot apply it",
        );
    }
    (node["readOnly"] == true).then_some("The published schema marks this setting read-only")
}

struct Field {
    key: String,
    choices: Vec<String>,
    reason: Option<&'static str>,
}
fn fields(
    root: &Value,
    node: &Value,
    prefix: &str,
    reason: Option<&'static str>,
    depth: usize,
    out: &mut Vec<Field>,
) {
    if depth > MAX_DEPTH || out.len() >= MAX_FIELDS {
        return;
    }
    let reason = reason.or_else(|| read_only_reason(node));
    if let Some(reference) = node["$ref"].as_str() {
        // Never load external references. The full JSON editor covers unknown shapes.
        if let Some(pointer) = reference.strip_prefix('#') {
            if let Some(target) = root.pointer(pointer) {
                fields(root, target, prefix, reason, depth + 1, out);
                return;
            }
        }
    }
    if let Some(properties) = node["properties"].as_object().filter(|p| !p.is_empty()) {
        for (key, child) in properties {
            if sensitive(key) || key.starts_with('$') || key.starts_with('_') {
                continue;
            }
            let key = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            fields(root, child, &key, reason, depth + 1, out);
        }
    } else if !prefix.is_empty() {
        // Do not restrict open-ended anyOf/oneOf strings to one enum branch.
        let choices = node["enum"]
            .as_array()
            .filter(|values| values.iter().all(Value::is_string))
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        out.push(Field {
            key: prefix.into(),
            choices,
            reason,
        });
    }
}
fn append_schema(response: &mut p::ConfigReadResponse, schema: &Value, source: &str) {
    let mut declared = Vec::new();
    fields(schema, schema, "", None, 0, &mut declared);
    for field in declared {
        let configured = config_contains(&response.config, &field.key);
        let Some(rows) = response.config["_litterSettings"].as_array_mut() else {
            return;
        };
        // A configured object/array may be a JSON row rather than a schema leaf.
        for row in rows.iter_mut().filter(|row| {
            row["key"]
                .as_str()
                .is_some_and(|key| key == field.key || key.starts_with(&format!("{}.", field.key)))
        }) {
            if let Some(reason) = field.reason {
                row["writable"] = json!(false);
                row["readOnlyReason"] = json!(reason);
                row["scope"] = json!("native source is read-only here");
            }
            // The installed runtime remains authoritative for already-set values,
            // so a potentially lagging schema does not constrain its choices.
        }
        if configured {
            continue;
        }
        rows.push(json!({"key":field.key,"label":field.key,"valueJson":"null","valueKind":"json",
            "choices":field.choices,"scope":if field.reason.is_some(){"native source is read-only here"}else{"user override (unset)"},
            "source":source,"writable":field.reason.is_none(),"readOnlyReason":field.reason}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_schema_preserves_values_and_declares_unset_scoped_fields() {
        let mut response = super::super::response(
            json!({"model":"future-model", "futureKey":5, "managedFlag":true, "apiKey":"PRIVATE"}),
            "native-file",
            true,
            None,
        );
        let schema = json!({"properties":{
            "model":{"type":"string","enum":["old-model"]},
            "theme":{"type":"string","default":"dark","enum":["dark","light"]},
            "nested":{"properties":{"toggle":{"type":"boolean","default":true}}},
            "managedFlag":{"type":"boolean","description":"(Managed settings only) Policy"},
            "policy":{"description":"Only honored from managed (policy) settings.","properties":{"child":{"type":"string"}}},
            "diffTool":{"description":"See docs#global-config-settings","type":"string"},
            "apiKey":{"type":"string"},"env":{"properties":{"TOKEN":{"type":"string"}}},
            "list":{"type":"array","items":{"type":"object","properties":{"token":{"type":"string"}}}},
            "referenced":{"$ref":"#/$defs/local"}, "external":{"$ref":"https://example.invalid/schema"}
        },"$defs":{"local":{"properties":{"mode":{"enum":["one","two"]}}}}});
        append_schema(&mut response, &schema, SOURCE);
        let rows = response.config["_litterSettings"].as_array().unwrap();
        let find = |key: &str| rows.iter().find(|v| v["key"] == key).unwrap();
        assert_eq!(find("theme")["valueJson"], "null");
        assert_eq!(find("theme")["choices"], json!(["dark", "light"]));
        assert_eq!(find("nested.toggle")["valueJson"], "null");
        assert_eq!(find("model")["valueJson"], r#""future-model""#);
        assert_eq!(find("model")["choices"], json!([]));
        assert_eq!(find("futureKey")["valueJson"], "5");
        for key in ["managedFlag", "policy.child", "diffTool"] {
            assert_eq!(find(key)["writable"], false);
        }
        assert_eq!(find("referenced.mode")["choices"], json!(["one", "two"]));
        assert_eq!(find("external")["valueJson"], "null");
        assert!(
            rows.iter()
                .all(|row| !row["key"].as_str().unwrap().starts_with("env")
                    && row["key"] != "apiKey")
        );
        assert!(!response.config.to_string().contains("PRIVATE"));
        let raw: Value =
            serde_json::from_str(find("$native")["valueJson"].as_str().unwrap()).unwrap();
        assert_eq!(
            raw,
            json!({"model":"future-model","futureKey":5,"managedFlag":true})
        );
    }

    #[test]
    fn factory_parser_uses_only_typed_personal_properties_and_never_defaults() {
        let document = br#"# Settings
<PropertyList>
<Property name='sound-label'>Not a setting</Property>
<Property name='showTokenUsageIndicator' type='boolean' defaultValue='false'>User setting</Property>
<Property type="string" name="missionModelSettings.workerModel" defaultValue="Inherits">User setting</Property>
<Property name='apiKey' type='string'>Secret</Property>
<Property name='env.TOKEN' type='string'>Secret</Property>
</PropertyList>
| `tableKey` | boolean | true | Not typed metadata |
## Enterprise and org-level settings
<Property name='managedPolicy' type='boolean'>Admin only</Property>
## Factory App usage alert preferences
<Property name='disableUsageLimitAlerts' type='boolean'>App only</Property>
"#;
        let schema = parse_droid_user_schema(document).unwrap();
        assert_eq!(schema["properties"].as_object().unwrap().len(), 2);
        assert!(
            schema["properties"]["showTokenUsageIndicator"]
                .get("default")
                .is_none()
        );
        let mut response =
            super::super::response(json!({"nativeUnknown":true}), "native-file", true, None);
        append_schema(&mut response, &schema, DROID_SOURCE);
        let rows = response.config["_litterSettings"].as_array().unwrap();
        assert_eq!(rows.len(), 4);
        for row in rows.iter().filter(|row| {
            row["key"] == "showTokenUsageIndicator"
                || row["key"] == "missionModelSettings.workerModel"
        }) {
            assert_eq!(row["valueJson"], "null");
            assert_eq!(row["writable"], true);
        }
        assert!(
            parse_droid_user_schema(
                b"<Property name='x' type='boolean'>No known scope boundary</Property>"
            )
            .is_err()
        );
    }

    #[tokio::test]
    #[ignore = "reads the officially published public metadata over HTTPS"]
    async fn published_claude_and_factory_sources_expose_unset_native_settings() {
        for (url, parser, source, minimum) in [
            (CLAUDE_SCHEMA, parse_json_schema as Parser, SOURCE, 100),
            (
                DROID_SCHEMA,
                parse_droid_user_schema as Parser,
                DROID_SOURCE,
                20,
            ),
        ] {
            let schema = SchemaCache::default()
                .read(url, parser)
                .await
                .expect("published metadata available");
            let mut response = super::super::response(
                json!({"unknownNativeOption":true,"apiKey":"TEST_ONLY_PRIVATE"}),
                "isolated fixture",
                true,
                None,
            );
            append_schema(&mut response, &schema, source);
            let rows = response.config["_litterSettings"].as_array().unwrap();
            assert!(rows.len() > minimum);
            assert!(!response.config.to_string().contains("TEST_ONLY_PRIVATE"));
            let declared = rows
                .iter()
                .find(|v| {
                    v["key"]
                        == if url == CLAUDE_SCHEMA {
                            "theme"
                        } else {
                            "showTokenUsageIndicator"
                        }
                })
                .unwrap();
            assert_eq!(declared["valueJson"], "null");
            assert_eq!(declared["scope"], "user override (unset)");
            if url == CLAUDE_SCHEMA {
                assert_eq!(
                    rows.iter()
                        .find(|v| v["key"] == "allowManagedPermissionRulesOnly")
                        .unwrap()["writable"],
                    false
                );
                assert_eq!(
                    rows.iter()
                        .find(|v| v["key"] == "permissions.defaultMode")
                        .unwrap()["writable"],
                    true
                );
            }
            println!(
                "{url}: {} descriptors, unset values and redaction verified",
                rows.len()
            );
        }
    }

    async fn server(replies: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for reply in replies {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = vec![0; 4096];
                let n = stream.read(&mut bytes).await.unwrap();
                requests.push(String::from_utf8_lossy(&bytes[..n]).into_owned());
                stream.write_all(reply.as_bytes()).await.unwrap();
            }
            requests
        });
        (url, task)
    }
    #[tokio::test]
    async fn cached_schema_revalidates_and_survives_failure() {
        let body = r#"{"properties":{"theme":{"type":"string"}}}"#;
        let (url,task)=server(vec![
            format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nConnection: close\r\n\r\n{body}",body.len()),
            "HTTP/1.1 304 Not Modified\r\nConnection: close\r\n\r\n".into(),
            "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        ]).await;
        let mut cache = SchemaCache::default();
        let original = cache.read(&url, parse_json_schema).await.unwrap();
        assert_eq!(cache.read(&url, parse_json_schema).await.unwrap(), original); // no request while fresh
        cache.refresh_after = None;
        assert_eq!(cache.read(&url, parse_json_schema).await.unwrap(), original);
        cache.refresh_after = None;
        assert_eq!(cache.read(&url, parse_json_schema).await.unwrap(), original);
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert!(
            requests[1]
                .to_lowercase()
                .contains("if-none-match: \"fixture\"")
        );
    }
    #[tokio::test]
    async fn invalid_or_oversized_schema_keeps_native_fallback() {
        for reply in [
            "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".to_string(),
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                MAX_BYTES + 1
            ),
        ] {
            let (url, task) = server(vec![reply]).await;
            assert!(
                SchemaCache::default()
                    .read(&url, parse_json_schema)
                    .await
                    .is_none()
            );
            task.await.unwrap();
        }
    }
}
