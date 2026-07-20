use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_JSONL_FRAME_BYTES: usize = 1 << 20;

#[derive(Debug, thiserror::Error)]
pub enum ReadJsonLineError {
    #[error("reading JSONL frame: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSONL frame exceeds {limit} byte limit")]
    Oversized { limit: usize },
    #[error("decoding JSONL frame: {0}")]
    Json(#[from] serde_json::Error),
}

pub async fn read_json_line<T, R>(reader: &mut R) -> Result<Option<T>, ReadJsonLineError>
where
    T: DeserializeOwned,
    R: AsyncBufRead + Unpin,
{
    read_json_line_with_limit(reader, MAX_JSONL_FRAME_BYTES).await
}

pub async fn read_json_line_with_limit<T, R>(
    reader: &mut R,
    limit: usize,
) -> Result<Option<T>, ReadJsonLineError>
where
    T: DeserializeOwned,
    R: AsyncBufRead + Unpin,
{
    let mut line = Vec::new();
    loop {
        line.clear();
        loop {
            let buffer = reader.fill_buf().await?;
            if buffer.is_empty() {
                if line.is_empty() {
                    return Ok(None);
                }
                break;
            }
            let newline = buffer.iter().position(|byte| *byte == b'\n');
            let payload_bytes = newline.unwrap_or(buffer.len());
            if line.len().saturating_add(payload_bytes) > limit {
                return Err(ReadJsonLineError::Oversized { limit });
            }
            line.extend_from_slice(&buffer[..payload_bytes]);
            let consumed = payload_bytes + usize::from(newline.is_some());
            reader.consume(consumed);
            if newline.is_some() {
                break;
            }
        }
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        return Ok(Some(serde_json::from_slice(&line)?));
    }
}

pub async fn write_json_line<T, W>(writer: &mut W, value: &T) -> anyhow::Result<()>
where
    T: Serialize,
    W: AsyncWrite + Unpin,
{
    let line = serde_json::to_vec(value)?;
    writer.write_all(&line).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}
