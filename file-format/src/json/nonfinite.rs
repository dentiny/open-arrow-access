use jiter::{Jiter, Peek};
use std::{borrow::Cow, ops::Range};

/// Adapts bare `NaN`, `Infinity`, and `-Infinity` values without changing strings.
/// The JSON parser still validates the result; malformed tokens and keys stay invalid.
#[must_use]
pub(super) fn normalize_nonfinite(line: &[u8]) -> Cow<'_, [u8]> {
    let mut parser = Jiter::new(line).with_allow_inf_nan();
    let mut numbers = Vec::new();
    // Leave invalid or incomplete JSON unchanged so the existing reader reports
    // its usual error (including EOF detection for multiline documents).
    // Bound recursion while inspecting nested objects and lists.
    if collect_nonfinite(&mut parser, &mut numbers, 128).is_none()
        || parser.finish().is_err()
        || numbers.is_empty()
    {
        return Cow::Borrowed(line);
    }

    let mut output = Vec::with_capacity(line.len());
    let mut copied = 0;
    for range in numbers {
        output.extend_from_slice(&line[copied..range.start]);
        output.push(b'"');
        output.extend_from_slice(&line[range.clone()]);
        output.push(b'"');
        copied = range.end;
    }
    output.extend_from_slice(&line[copied..]);
    Cow::Owned(output)
}

/// Walk JSON values using the parser's object/array APIs. Jiter handles syntax,
/// strings and escapes; we only record the byte ranges of nonfinite numbers.
fn collect_nonfinite(
    parser: &mut Jiter<'_>,
    numbers: &mut Vec<Range<usize>>,
    depth: usize,
) -> Option<()> {
    if depth == 0 {
        return None;
    }
    match parser.peek().ok()? {
        Peek::Object => {
            let mut has_key = parser.next_object().ok()?.is_some();
            while has_key {
                collect_nonfinite(parser, numbers, depth - 1)?;
                has_key = parser.next_key().ok()?.is_some();
            }
        }
        Peek::Array => {
            let mut element = parser.next_array().ok()?;
            while element.is_some() {
                collect_nonfinite(parser, numbers, depth - 1)?;
                element = parser.array_step().ok()?;
            }
        }
        _ => {
            let start = parser.current_index();
            parser.next_skip().ok()?;
            if matches!(
                parser.slice_to_current(start),
                b"NaN" | b"Infinity" | b"-Infinity"
            ) {
                numbers.push(start..parser.current_index());
            }
        }
    }
    Some(())
}
