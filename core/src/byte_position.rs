/// Coordinates of a byte in stored or frame-decoded input.
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
