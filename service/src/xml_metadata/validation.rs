use super::error;
use open_arrow_access_core::Result;
use quick_xml::{Reader, events::Event};

/// Check the first document element's local name without rewriting input bytes.
/// Declarations, comments, whitespace and a DTD are accepted. This does not
/// replace full document parsing, schema validation or namespace validation.
pub fn validate_root(bytes: &[u8], expected: &str) -> Result<()> {
    let mut reader = Reader::from_reader(bytes);
    loop {
        match reader.read_event().map_err(error::parse)? {
            Event::Start(element) | Event::Empty(element)
                if element.local_name().as_ref() == expected.as_bytes() =>
            {
                return Ok(());
            }
            Event::Decl(_) | Event::Comment(_) | Event::DocType(_) => {}
            Event::Text(text) if text.iter().all(u8::is_ascii_whitespace) => {}
            _ => return Err(error::root(expected)),
        }
    }
}
