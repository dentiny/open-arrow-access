use glob::Pattern;
use open_arrow_access_core::{Error, ErrorKind, Result};

pub(super) fn compile_glob(pattern: Option<&str>) -> Result<Option<Pattern>> {
    pattern.map(Pattern::new).transpose().map_err(|error| {
        Error::new(
            ErrorKind::InvalidInput,
            "select_directory",
            error.to_string(),
        )
        .with_source(error)
    })
}
