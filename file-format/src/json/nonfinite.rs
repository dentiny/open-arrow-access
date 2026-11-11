use std::borrow::Cow;

/// Quote nonfinite values for Arrow, leaving strings and all other bytes intact.
/// JsonFrames has already validated the complete object, so token boundaries and
/// string escapes are known to be valid here.
pub(super) fn normalize_nonfinite(bytes: &[u8]) -> Cow<'_, [u8]> {
    let mut output = Vec::new();
    let mut copied = 0;
    let mut index = 0;
    while index < bytes.len() {
        let length = match bytes[index] {
            b'"' => {
                index += 1;
                while bytes[index] != b'"' {
                    if bytes[index] == b'\\' {
                        index += 1;
                    }
                    index += 1;
                }
                0
            }
            b'N' => 3,
            b'I' => 8,
            b'-' if bytes[index..].starts_with(b"-Infinity") => 9,
            _ => 0,
        };
        if length == 0 {
            index += 1;
            continue;
        }
        output.extend_from_slice(&bytes[copied..index]);
        output.push(b'"');
        output.extend_from_slice(&bytes[index..index + length]);
        output.push(b'"');
        index += length;
        copied = index;
    }
    if copied == 0 {
        Cow::Borrowed(bytes)
    } else {
        output.extend_from_slice(&bytes[copied..]);
        Cow::Owned(output)
    }
}
