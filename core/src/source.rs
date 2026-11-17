//! Byte coordinates and opt-in record provenance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BytePosition {
    Stored(u64),
    Frame { frame: u64, decoded: u64 },
}
impl BytePosition {
    pub fn advance(self, n: u64) -> Self {
        match self {
            Self::Stored(offset) => Self::Stored(offset + n),
            Self::Frame { frame, decoded } => Self::Frame {
                frame,
                decoded: decoded + n,
            },
        }
    }
    pub fn offset(self) -> u64 {
        match self {
            Self::Stored(offset) => offset,
            Self::Frame { frame, .. } => frame,
        }
    }
    pub fn offset_in_frame(self) -> u64 {
        match self {
            Self::Stored(_) => 0,
            Self::Frame { decoded, .. } => decoded,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SourceColumn {
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
