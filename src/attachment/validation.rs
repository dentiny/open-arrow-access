use super::{
    AttachmentFormat, AttachmentOptions,
    error::{invalid, missing_format},
};
use crate::{OuterCompression, Result};

pub(super) fn validate(options: &AttachmentOptions) -> Result<()> {
    if options.format == AttachmentFormat::Binary
        && options.outer_compression != OuterCompression::None
    {
        return Err(invalid("binary attachments require OuterCompression::None"));
    }
    if let AttachmentFormat::Xml { record_path } = &options.format
        && record_path.trim().is_empty()
    {
        return Err(invalid("XML attachments require a nonempty record path"));
    }
    let (feature, enabled) = match &options.format {
        AttachmentFormat::Binary => return Ok(()),
        AttachmentFormat::Csv | AttachmentFormat::Tsv => {
            ("format-csv", cfg!(feature = "format-csv"))
        }
        AttachmentFormat::Json | AttachmentFormat::Jsonl => {
            ("format-json", cfg!(feature = "format-json"))
        }
        AttachmentFormat::Parquet => ("format-parquet", cfg!(feature = "format-parquet")),
        AttachmentFormat::Xml { .. } => ("format-xml", cfg!(feature = "format-xml")),
    };
    if enabled {
        Ok(())
    } else {
        Err(missing_format(feature))
    }
}
