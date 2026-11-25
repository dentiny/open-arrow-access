//! Remove a physical-line preamble before either inference or decoding sees it.

use futures::stream;
use open_arrow_access_core::{ByteReader, StreamReader};

pub(super) fn skip_rows(input: Box<dyn ByteReader>, rows: usize) -> Box<dyn ByteReader> {
    if rows == 0 {
        return input;
    }
    let mut metadata = input.metadata().clone();
    metadata.content_length = None;
    let chunks = stream::try_unfold(
        (input, rows, false),
        |(mut input, mut remaining, mut after_cr)| async move {
            while let Some(bytes) = input.next().await? {
                let mut offset = 0;
                while offset < bytes.len() {
                    if after_cr {
                        after_cr = false;
                        if bytes[offset] == b'\n' {
                            offset += 1;
                            continue;
                        }
                    }
                    if remaining == 0 {
                        break;
                    }
                    match bytes[offset] {
                        b'\r' => {
                            remaining -= 1;
                            after_cr = true;
                        }
                        b'\n' => remaining -= 1,
                        _ => {}
                    }
                    offset += 1;
                }
                if offset < bytes.len() {
                    return Ok(Some((bytes.slice(offset..), (input, remaining, after_cr))));
                }
            }
            Ok(None)
        },
    );
    Box::new(StreamReader::bytes(chunks, metadata))
}
