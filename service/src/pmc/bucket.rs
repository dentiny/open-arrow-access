use open_arrow_access_core::{Error, ErrorKind, Result};
use opendal::{Operator, services::S3};

/// Root of the public bucket referenced by PMC content URLs.
pub const BUCKET_URL: &str = "s3://pmc-oa-opendata/";

/// An anonymous, bucket-root operator for metadata and article content.
///
/// Disables ambient credential/config loading and request signing. Region is
/// independent of any destination storage. No I/O occurs here; callers may layer
/// their own retry/timeout policies before sharing this operator with a sink.
pub fn bucket_operator() -> Result<Operator> {
    opendal::install_default();
    Operator::new(
        S3::default()
            .bucket("pmc-oa-opendata")
            .region("us-east-1")
            .disable_config_load()
            .skip_signature(),
    )
    .map_err(|error| {
        Error::new(
            ErrorKind::InvalidInput,
            "configure_pmc",
            "public PMC bucket",
        )
        .with_source(error)
    })
}
