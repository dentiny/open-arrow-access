use super::error;
use open_arrow_access_core::Result;

pub(super) fn outside_root(allowed: bool) -> Result<()> {
    if !allowed {
        return Err(error::invalid("content outside the document element"));
    }
    Ok(())
}

pub(super) fn enter(
    root_seen: bool,
    depth: usize,
    max_depth: usize,
    byte_offset: u64,
) -> Result<()> {
    if depth == max_depth {
        return Err(error::limit(
            xml_arrow::Limit::Depth,
            max_depth,
            byte_offset,
        ));
    }
    if depth == 0 && root_seen {
        return Err(error::invalid("multiple document elements"));
    }
    Ok(())
}

pub(super) fn eof(root_seen: bool, depth: usize) -> Result<()> {
    if !root_seen || depth != 0 {
        return Err(error::invalid("expected one complete XML document"));
    }
    Ok(())
}
