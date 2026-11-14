use futures::{StreamExt, stream, stream::BoxStream};
use open_arrow_access_core::{ByteReader, Bytes, Result};

use super::error::empty_chunk;

/// Frame complete JSON objects without relying on extensions or line boundaries.
pub(super) fn documents(input: Box<dyn ByteReader>) -> BoxStream<'static, Result<Bytes>> {
    let state = Documents {
        input,
        pending: Bytes::new(),
    };
    stream::try_unfold(state, |mut state| async move {
        Ok(state
            .next_document()
            .await?
            .map(|document| (document, state)))
    })
    .boxed()
}

struct Documents {
    input: Box<dyn ByteReader>,
    pending: Bytes,
}

impl Documents {
    async fn next_document(&mut self) -> Result<Option<Bytes>> {
        let mut document = Vec::new();
        let mut depth = 0usize;
        let mut quoted = false;
        let mut escaped = false;
        loop {
            if self.pending.is_empty() {
                match self.input.next().await? {
                    Some(bytes) if bytes.is_empty() => return Err(empty_chunk()),
                    Some(bytes) => self.pending = bytes,
                    None => {
                        // Let the trajectory decoder report an incomplete final object.
                        return Ok((!document.is_empty()).then(|| Bytes::from(document)));
                    }
                }
            }
            if document.is_empty() {
                let whitespace = self
                    .pending
                    .iter()
                    .take_while(|byte| matches!(byte, b' ' | b'\t' | b'\r' | b'\n'))
                    .count();
                self.pending = self.pending.slice(whitespace..);
                if self.pending.is_empty() {
                    continue;
                }
                if self.pending[0] != b'{' {
                    // Keep type/UTF-8 errors in the existing trajectory decoder.
                    return Ok(Some(std::mem::take(&mut self.pending)));
                }
            }
            for (index, byte) in self.pending.iter().copied().enumerate() {
                if quoted {
                    if escaped {
                        escaped = false;
                    } else if byte == b'\\' {
                        escaped = true;
                    } else if byte == b'"' {
                        quoted = false;
                    }
                } else {
                    match byte {
                        b'"' => quoted = true,
                        b'{' | b'[' => depth += 1,
                        b'}' | b']' => depth -= 1,
                        _ => {}
                    }
                }
                if depth == 0 && !quoted {
                    let bytes = self.pending.split_to(index + 1);
                    if document.is_empty() {
                        return Ok(Some(bytes));
                    }
                    document.extend_from_slice(&bytes);
                    return Ok(Some(Bytes::from(document)));
                }
            }
            document.extend_from_slice(&std::mem::take(&mut self.pending));
        }
    }
}
