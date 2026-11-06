use std::io;

use arrow_schema::ArrowError;
use open_arrow_access_core::{Error, ErrorKind};

#[test]
fn shared_categories_survive_io_and_arrow_error_wrappers() {
    for kind in [
        ErrorKind::InvalidInput,
        ErrorKind::Unsupported,
        ErrorKind::NotFound,
        ErrorKind::PermissionDenied,
        ErrorKind::Io,
        ErrorKind::Decode,
    ] {
        let error = Error::new(kind, "read_fixture", "source failure");
        assert_eq!(ErrorKind::from_source(&error), Some(kind));
        let error = io::Error::other(error);
        assert_eq!(ErrorKind::from_source(&error), Some(kind));
        let error = io::Error::other(error);
        assert_eq!(ErrorKind::from_source(&error), Some(kind));
        let error = ArrowError::ExternalError(Box::new(error));
        assert_eq!(ErrorKind::from_source(&error), Some(kind));
    }
}

#[test]
fn the_outer_shared_error_category_takes_precedence() {
    let error = Error::new(ErrorKind::Io, "fetch", "read failed").with_source(Error::new(
        ErrorKind::PermissionDenied,
        "backend",
        "denied",
    ));
    assert_eq!(ErrorKind::from_source(&error), Some(ErrorKind::Io));
}

#[test]
fn unclassified_upstream_errors_do_not_invent_a_shared_category() {
    let error = io::Error::new(io::ErrorKind::NotFound, "plain upstream error");
    assert_eq!(ErrorKind::from_source(&error), None);
    let error = ArrowError::ParseError("invalid input bytes".into());
    assert_eq!(ErrorKind::from_source(&error), None);
}
