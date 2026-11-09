use futures::{Stream, stream};

use crate::{ByteReader, Bytes, Error, ErrorKind, Result};

/// Stream lines from an owned byte reader, retaining LF and CRLF endings.
///
/// A nonempty final line is emitted without a newline; a trailing newline adds
/// no extra line. Bytes are not decoded and blank lines are not filtered.
/// `max_line_bytes` includes line endings; `None` allows unbounded lines.
/// The limit bounds line bytes, not allocation capacity or upstream buffers.
/// Construction performs no I/O. EOF and errors stop reading and drop the input;
/// dropping the stream also drops the input without background work.
///
/// # Errors
///
/// Oversized lines return [`ErrorKind::Decode`] before copying the offending
/// bytes. Empty chunks return [`ErrorKind::Io`]. Both use `operation`;
/// upstream errors pass through unchanged. An error ends the stream.
///
/// ```no_run
/// use futures::TryStreamExt;
/// use open_arrow_access_core::{ByteReader, Result, read_lines};
///
/// # async fn read_manifest(input: Box<dyn ByteReader>) -> Result<()> {
/// let lines = read_lines(input, Some(16 * 1024), "read_manifest");
/// futures::pin_mut!(lines);
/// while let Some(line) = lines.try_next().await? {
///     // Parse the bounded line here.
/// }
/// # Ok(())
/// # }
/// ```
pub fn read_lines<R>(
    input: Box<R>,
    max_line_bytes: Option<usize>,
    operation: &'static str,
) -> impl Stream<Item = Result<Bytes>> + Send
where
    R: ByteReader + ?Sized,
{
    let state = LineState {
        input: Some(input),
        pending: Bytes::new(),
    };
    stream::try_unfold(state, move |mut state| async move {
        Ok(state
            .read_line(max_line_bytes, operation)
            .await?
            .map(|line| (line, state)))
    })
}

struct LineState<R: ?Sized> {
    input: Option<Box<R>>,
    pending: Bytes,
}

impl<R: ByteReader + ?Sized> LineState<R> {
    async fn read_line(
        &mut self,
        max_line_bytes: Option<usize>,
        operation: &'static str,
    ) -> Result<Option<Bytes>> {
        let mut line = Vec::new();
        loop {
            if self.pending.is_empty() {
                let Some(input) = self.input.as_mut() else {
                    return Ok((!line.is_empty()).then(|| Bytes::from(line)));
                };
                match input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(Error::new(
                            ErrorKind::Io,
                            operation,
                            "byte reader returned an empty chunk",
                        ));
                    }
                    Some(bytes) => self.pending = bytes,
                    None => {
                        self.input = None;
                        continue;
                    }
                }
            }
            let newline = memchr::memchr(b'\n', &self.pending);
            let consumed = newline.map_or(self.pending.len(), |index| index + 1);
            if let Some(limit) = max_line_bytes
                && consumed > limit - line.len()
            {
                return Err(Error::new(
                    ErrorKind::Decode,
                    operation,
                    format!("line exceeds configured {limit}-byte limit"),
                ));
            }
            let bytes = self.pending.split_to(consumed);
            if line.is_empty() && newline.is_some() {
                return Ok(Some(bytes));
            }
            line.extend_from_slice(&bytes);
            if newline.is_some() {
                return Ok(Some(Bytes::from(line)));
            }
        }
    }
}
