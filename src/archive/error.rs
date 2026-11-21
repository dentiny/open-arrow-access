use crate::Error;

pub(super) fn empty_chunk() -> Error {
    crate::empty_chunk("read_archive")
}
