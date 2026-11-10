//! Shared rules for one logical filename component, without URL encoding.

/// Accept a nonempty component without separators, dot components, control
/// characters or surrounding whitespace. Provider-specific errors stay with callers.
pub(crate) fn valid_filename_component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains(['/', '\\'])
        && !value.chars().any(char::is_control)
        && value.trim() == value
}
