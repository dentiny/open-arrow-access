use futures::stream;
use memchr::memchr2;
use open_arrow_access_core::{ByteReader, Bytes, StreamReader, empty_chunk, next_nonempty};

/// Adapt nonfinite value tokens before either serde_json framing or Arrow decoding.
/// Keep only a partial token between chunks; the downstream parser validates JSON.
pub(super) fn normalize_input(input: Box<dyn ByteReader>) -> Box<dyn ByteReader> {
    let mut metadata = input.metadata().clone();
    metadata.content_length = None;
    let stream = stream::try_unfold(
        (input, Normalizer::default()),
        |(mut input, mut state)| async move {
            loop {
                let output =
                    match next_nonempty(input.as_mut(), || empty_chunk("read_json")).await? {
                        Some(bytes) => state.decode(&bytes),
                        None if state.token.is_empty() => return Ok(None),
                        None => state.take_token(),
                    };
                if !output.is_empty() {
                    return Ok(Some((Bytes::from(output), (input, state))));
                }
            }
        },
    );
    Box::new(StreamReader::new(stream, metadata))
}

/// Produce finite placeholders for type inference, keeping raw byte offsets and
/// quoted strings unchanged. An incomplete scalar waits for more input unless EOF.
pub(super) fn inference_sample(bytes: &[u8], eof: bool) -> Vec<u8> {
    let mut state = Normalizer {
        inference: true,
        ..Normalizer::default()
    };
    let mut output = state.decode(bytes);
    if eof {
        output.extend(state.take_token());
    }
    output
}

#[derive(Default)]
struct Normalizer {
    inference: bool,
    in_string: bool,
    escaped: bool,
    // Whether the current container is an array; commas in objects precede keys.
    arrays: Vec<bool>,
    value: bool,
    token: Vec<u8>,
}

impl Normalizer {
    fn take_token(&mut self) -> Vec<u8> {
        let mut token = std::mem::take(&mut self.token);
        if matches!(token.as_slice(), b"NaN" | b"Infinity" | b"-Infinity") {
            if self.inference {
                let length = token.len();
                token.clear();
                token.extend_from_slice(b"0.0");
                token.resize(length, b' ');
            } else {
                token.insert(0, b'"');
                token.push(b'"');
            }
        }
        self.value = false;
        token
    }

    fn decode(&mut self, mut bytes: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(bytes.len());
        while !bytes.is_empty() {
            if self.in_string && !self.escaped {
                // Only a quote or backslash changes string state. Long document
                // text can be scanned with memchr and copied as one slice instead
                // of branching and pushing every byte. Preserve partial escapes
                // across input chunks; downstream decoding still validates JSON.
                let end = memchr2(b'"', b'\\', bytes).unwrap_or(bytes.len());
                output.extend_from_slice(&bytes[..end]);
                bytes = &bytes[end..];
                if bytes.is_empty() {
                    break;
                }
            }
            let byte = bytes[0];
            bytes = &bytes[1..];
            if !self.token.is_empty() {
                if !matches!(
                    byte,
                    b' ' | b'\r' | b'\n' | b'\t' | b',' | b':' | b'{' | b'}' | b'[' | b']' | b'"'
                ) {
                    self.token.push(byte);
                    continue;
                }
                // Forward complete scalars so framing never sees half a number (e.g. 1e-).
                output.extend(self.take_token());
            }
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
            } else {
                match byte {
                    b'"' => {
                        self.in_string = true;
                        self.value = false;
                    }
                    b'{' | b'[' => {
                        self.arrays.push(byte == b'[');
                        self.value = byte == b'[';
                    }
                    b'}' | b']' => {
                        self.arrays.pop();
                        self.value = false;
                    }
                    b':' => self.value = true,
                    b',' => self.value = self.arrays.last() == Some(&true),
                    b' ' | b'\r' | b'\n' | b'\t' => {}
                    _ if self.value => {
                        self.token.push(byte);
                        continue;
                    }
                    _ => self.value = false,
                }
            }
            output.push(byte);
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::Normalizer;

    fn normalize<'a>(chunks: impl IntoIterator<Item = &'a [u8]>, inference: bool) -> Vec<u8> {
        let mut state = Normalizer {
            inference,
            ..Normalizer::default()
        };
        let mut output = Vec::new();
        for chunk in chunks {
            output.extend(state.decode(chunk));
        }
        output.extend(state.take_token());
        output
    }

    #[test]
    fn quoted_bytes_and_nonfinite_values_survive_every_chunk_boundary() {
        let input = r#"{"text":"正文 \"quoted\" \\path\u0000 {[,:]} NaN Infinity -Infinity","v":[NaN,Infinity,-Infinity,1e-3]}"#.as_bytes();
        let expected = r#"{"text":"正文 \"quoted\" \\path\u0000 {[,:]} NaN Infinity -Infinity","v":["NaN","Infinity","-Infinity",1e-3]}"#.as_bytes();
        let inferred = r#"{"text":"正文 \"quoted\" \\path\u0000 {[,:]} NaN Infinity -Infinity","v":[0.0,0.0     ,0.0      ,1e-3]}"#.as_bytes();
        for split in 0..=input.len() {
            let chunks = [&input[..split], &input[split..]];
            assert_eq!(normalize(chunks, false), expected, "read split {split}");
            assert_eq!(normalize(chunks, true), inferred, "inference split {split}");
        }
    }

    #[test]
    fn long_quoted_runs_are_unchanged_in_both_modes() {
        let text = "ordinary UTF-8 正文 NaN Infinity ".repeat(4096);
        let input = format!(r#"{{"text":"{text}"}}"#).into_bytes();
        for size in [1, 7, 4095, 4096, 4097, input.len()] {
            for inference in [false, true] {
                assert_eq!(normalize(input.chunks(size), inference), input);
            }
        }
    }
}
