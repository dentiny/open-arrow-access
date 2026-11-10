/// Operations exposed by the current accessor or reader, following OpenDAL's pattern.
/// Transforming layers update the flags to match their output interface.
/// Assign every flag explicitly; unset flags mean unsupported.
///
/// Accessors report support before opening; readers report the opened representation.
/// Reporting performs no I/O and guarantees neither authorization nor snapshot stability.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Capability {
    /// Supports sequential consumption through [`crate::Reader::next`].
    pub read: bool,
    /// Supports [`crate::ByteReader::read_range`] without moving the sequential cursor.
    /// Implies `read`; always false for record interfaces.
    pub read_with_range: bool,
    /// Supports concurrent files or partitions within this interface; implies `read`.
    /// Independent of range access, output ordering, per-reader I/O concurrency and
    /// caller-managed concurrency across readers. Reporting does not enable execution.
    // TODO: Clarify whether this flag means support for caller-managed parallel
    // scheduling or implemented internal parallel execution before enabling it.
    pub read_parallel: bool,
    /// Supports [`crate::ByteAccessor::list`]; always false for readers.
    pub list: bool,
}
