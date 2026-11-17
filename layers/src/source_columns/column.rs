/// An opt-in record provenance column.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SourceColumn {
    Object,
    Offset,
    OffsetInFrame,
}
impl SourceColumn {
    pub fn name(self) -> &'static str {
        match self {
            Self::Object => "_source_object",
            Self::Offset => "_source_offset",
            Self::OffsetInFrame => "_source_offset_in_frame",
        }
    }
    pub fn field(self) -> arrow_schema::Field {
        arrow_schema::Field::new(
            self.name(),
            match self {
                Self::Object => arrow_schema::DataType::Utf8,
                _ => arrow_schema::DataType::UInt64,
            },
            false,
        )
        .with_metadata(
            [(
                "open_arrow_access:source_column".to_owned(),
                self.name().to_owned(),
            )]
            .into(),
        )
    }
}
