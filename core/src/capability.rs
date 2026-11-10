/// Operations exposed by an accessor or reader after all adapters and layers.
///
/// This follows OpenDAL's capability-reporting pattern. Flags describe the
/// current interface, not every operation supported by its underlying backend.
/// Transparent layers forward them; transforming layers must update them to
/// match the operations they actually expose. Unset flags mean unsupported.
/// Use [`Capability::sequential`] for sequential-only interfaces; otherwise
/// assign flags explicitly to describe the exposed operations.
///
/// Accessors report supported operations before opening. Readers report the
/// abilities of the opened representation, which can be more specific. `read`
/// describes sequential consumption; `read_with_range` adds exact byte ranges.
/// Readers never advertise `list`, which is an accessor-only operation.
///
/// A supported operation can still fail for a particular request, for example
/// because of permissions, missing input, invalid options or unknown length.
/// Capabilities perform no I/O and provide no authorization or snapshot guarantee.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Capability {
    /// Can open/read the interface's item type through [`crate::Reader::next`].
    pub read: bool,
    /// Can read exact ranges in the current byte representation without moving
    /// the sequential cursor, through [`crate::ByteReader::read_range`].
    /// Implies `read`; always false for record interfaces.
    pub read_with_range: bool,
    /// Can read multiple files or partitions concurrently within this interface.
    /// Implies `read`; independent of range access and output ordering.
    /// Does not enable execution or describe per-reader byte I/O concurrency
    /// or caller-managed concurrency across separate readers.
    // TODO: Clarify whether this flag means support for caller-managed parallel
    // scheduling or implemented internal parallel execution before enabling it.
    pub read_parallel: bool,
    /// Can discover object references through [`crate::ByteAccessor::list`].
    /// Always false for readers and interfaces that do not expose discovery.
    pub list: bool,
}

impl Capability {
    /// Capabilities of an interface that exposes only sequential reads.
    pub const fn sequential(read: bool) -> Self {
        Self {
            read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }
}
