use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

pub(crate) fn invalid_frame(operation: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::Decode, operation, message)
}

pub(crate) fn json_frame(source: serde_json::Error) -> Error {
    Error::new(ErrorKind::Decode, "frame_json", source.to_string()).with_source(source)
}

pub(super) fn decode_error(error: ArrowError) -> Error {
    Error::new(ErrorKind::Decode, "decode_json", error.to_string()).with_source(error)
}

pub(super) fn invalid_inference_limits() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_json",
        "inference record and byte limits must be positive",
    )
}

pub(super) fn empty_inference_sample() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "infer_json",
        "cannot infer a schema from empty JSON input; supply an explicit schema",
    )
}

pub(super) fn non_object_inference_record() -> Error {
    Error::new(ErrorKind::Decode, "infer_json", "expected a JSON object")
}

pub(super) fn empty_inference_chunk() -> Error {
    Error::new(
        ErrorKind::Decode,
        "infer_json",
        "byte reader emitted an empty chunk",
    )
}

pub(super) fn inference_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::new(ErrorKind::Decode, "infer_json", source.to_string()).with_source(source)
}

pub(super) fn empty_read_chunk() -> Error {
    Error::new(
        ErrorKind::Io,
        "read_json",
        "byte reader emitted an empty chunk",
    )
}

pub(super) fn open_error(error: arrow_schema::ArrowError) -> Error {
    let kind = match &error {
        arrow_schema::ArrowError::NotYetImplemented(_) => ErrorKind::Unsupported,
        _ => ErrorKind::InvalidInput,
    };
    Error::new(kind, "open_json", error.to_string()).with_source(error)
}
