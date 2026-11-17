#[cfg(feature = "atif")]
mod accessor;
#[cfg(feature = "atif")]
mod auto;
#[cfg(feature = "atif")]
mod error;
#[cfg(feature = "atif")]
mod format;
#[cfg(feature = "atif")]
mod planning;
#[cfg(feature = "atif")]
mod reader;

#[cfg(feature = "atif")]
pub use accessor::AtifAccessor;

/// ATIF document framing; extensions are not inspected.
///
/// This identifier is always available. Reading ATIF requires the `atif` feature.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AtifFormat {
    /// One complete trajectory document, including pretty-printed JSON.
    Json,
    /// One complete trajectory per nonblank line. Arrays are not expanded.
    JsonLines,
    /// Frame JSON objects from their contents, accepting pretty JSON and JSONL.
    /// Inter-document whitespace is omitted from each row's raw_json.
    #[default]
    Auto,
}
