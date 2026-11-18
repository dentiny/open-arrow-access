use super::PathsManifest;
use open_arrow_access_core::{Error, ErrorKind};

pub(super) type PathResult<T> = std::result::Result<T, &'static str>;

impl PathsManifest {
    pub(super) fn open_error(&self, error: Error) -> Error {
        error.with_object(Some(&self.manifest))
    }

    pub(super) fn read_error(&self, number: usize, error: Error) -> Error {
        self.error(error.kind(), number, error.to_string())
            .with_source(error)
    }

    pub(super) fn duplicate_file(&self, number: usize) -> Error {
        self.error(ErrorKind::Decode, number, "duplicate selected file")
    }

    pub(super) fn empty_selection(&self, number: usize) -> Error {
        self.error(
            ErrorKind::InvalidInput,
            number,
            "manifest contains no selected files",
        )
    }

    pub(super) fn invalid_utf8(&self, number: usize, error: std::str::Utf8Error) -> Error {
        self.error(ErrorKind::Decode, number, "manifest is not UTF-8")
            .with_source(error)
    }

    pub(super) fn format_mismatch(&self, number: usize) -> Error {
        self.error(
            ErrorKind::Decode,
            number,
            "file format does not match selection",
        )
    }

    pub(super) fn error(
        &self,
        kind: ErrorKind,
        number: usize,
        message: impl std::fmt::Display,
    ) -> Error {
        Error::new(
            kind,
            "read_paths_manifest",
            format!("line {number}: {message}"),
        )
        .with_object(Some(&self.manifest))
    }
}
