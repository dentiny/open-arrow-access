use futures::stream;
use open_arrow_access_core::{ByteReader, Bytes, Error, ErrorKind, StreamReader};

/// Adapt nonfinite value tokens before either serde_json framing or Arrow decoding.
/// Keep only a partial token between chunks; the downstream parser validates JSON.
pub(super) fn normalize_input(input: Box<dyn ByteReader>) -> Box<dyn ByteReader> {
    let mut metadata = input.metadata().clone();
    metadata.content_length = None;
    let stream = stream::try_unfold(
        (input, Normalizer::default()),
        |(mut input, mut state)| async move {
            loop {
                let output = match input.next().await? {
                    Some(bytes) if bytes.is_empty() => {
                        return Err(Error::new(
                            ErrorKind::Io,
                            "read_json",
                            "byte reader emitted an empty chunk",
                        ));
                    }
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

#[derive(Default)]
struct Normalizer {
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
            token.insert(0, b'"');
            token.push(b'"');
        }
        self.value = false;
        token
    }

    fn decode(&mut self, bytes: &[u8]) -> Vec<u8> {
        let mut output = Vec::with_capacity(bytes.len());
        for &byte in bytes {
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
