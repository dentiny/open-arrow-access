use arrow_schema::{DataType, Field, Schema};
use open_arrow_access_core::SchemaRef;
use open_arrow_access_core::blob_bytes_field;
use std::sync::{Arc, OnceLock};

// Fixed BHL TSV source schema. Every cell retains its original lexical string.
macro_rules! record {
    ($($column:literal),+ $(,)?) => { DataType::Struct(vec![$(Field::new($column, DataType::LargeUtf8, false)),+].into()) };
}
macro_rules! many {
    ($($column:literal),+ $(,)?) => { DataType::List(Arc::new(Field::new("item", record!($($column),+), false))) };
}

/// Fixed page schema, including the 13-table / 104-field metadata contract.
/// Metadata is required and assigned from caller-supplied source values.
/// `page` and `part` retain every source record, including repeated IDs.
pub fn page_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            let metadata = vec![
                Field::new(
                    "title",
                    many!(
                        "TitleID",
                        "MARCBibID",
                        "MARCLeader",
                        "FullTitle",
                        "ShortTitle",
                        "PublicationDetails",
                        "CallNumber",
                        "StartYear",
                        "EndYear",
                        "LanguageCode",
                        "TL2Author",
                        "TitleURL",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "item",
                    many!(
                        "ItemID",
                        "TitleID",
                        "ThumbnailPageID",
                        "BarCode",
                        "MARCItemID",
                        "CallNumber",
                        "VolumeInfo",
                        "ItemURL",
                        "ItemTextURL",
                        "ItemPDFURL",
                        "ItemImagesURL",
                        "LocalID",
                        "Year",
                        "InstitutionName",
                        "ZQuery",
                        "CreationDate",
                        "CopyrightStatus",
                        "RightsStatement",
                        "LicenseType",
                        "RightsHolder"
                    ),
                    false,
                ),
                Field::new(
                    "page",
                    many!(
                        "PageID",
                        "ItemID",
                        "SequenceOrder",
                        "Year",
                        "Volume",
                        "Issue",
                        "PagePrefix",
                        "PageNumber",
                        "PageTypeName",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "part",
                    many!(
                        "PartID",
                        "ItemID",
                        "ContributorName",
                        "SequenceOrder",
                        "SegmentType",
                        "Title",
                        "ContainerTitle",
                        "PublicationDetails",
                        "Volume",
                        "Series",
                        "Issue",
                        "Date",
                        "PageRange",
                        "StartPageID",
                        "LanguageName",
                        "SegmentUrl",
                        "ExternalUrl",
                        "DownloadUrl",
                        "RightsStatus",
                        "RightsStatement",
                        "LicenseName",
                        "LicenseUrl",
                        "RightsHolder",
                        "BarCode"
                    ),
                    false,
                ),
                Field::new(
                    "partpage",
                    many!(
                        "PartID",
                        "PageID",
                        "ItemID",
                        "SequenceOrder",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "creator",
                    many!(
                        "TitleID",
                        "CreatorID",
                        "CreatorType",
                        "CreatorName",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "creatoridentifier",
                    many!(
                        "CreatorID",
                        "IdentifierName",
                        "IdentifierValue",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "subject",
                    many!("TitleID", "Subject", "CreationDate"),
                    false,
                ),
                Field::new(
                    "titleidentifier",
                    many!(
                        "TitleID",
                        "IdentifierName",
                        "IdentifierValue",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "partcreator",
                    many!("PartID", "CreatorID", "CreatorName", "CreationDate"),
                    false,
                ),
                Field::new(
                    "partidentifier",
                    many!(
                        "PartID",
                        "IdentifierName",
                        "IdentifierValue",
                        "CreationDate"
                    ),
                    false,
                ),
                Field::new(
                    "pagename",
                    many!("NameBankID", "NameConfirmed", "PageID", "CreationDate"),
                    false,
                ),
                Field::new(
                    "doi",
                    many!("EntityType", "EntityID", "DOI", "CreationDate"),
                    false,
                ),
            ];
            Arc::new(Schema::new(vec![
                Field::new("entity_type", DataType::Utf8, false),
                Field::new("entity_id", DataType::UInt64, false),
                Field::new("page_id", DataType::UInt64, false),
                Field::new("sequence", DataType::UInt64, false),
                blob_bytes_field(Field::new("data", DataType::LargeBinary, false)),
                Field::new("source_path", DataType::LargeUtf8, false),
                Field::new("snapshot_id", DataType::Utf8, false),
                Field::new("source_entry_index", DataType::UInt64, true),
                Field::new("metadata", DataType::Struct(metadata.into()), false),
            ]))
        })
        .clone()
}
