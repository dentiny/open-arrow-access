/// Byte/generic operations exposed after all adapters and layers.
/// Record interfaces use [`RecordCapability`] with an explicit ordering contract.
///
/// This follows OpenDAL's capability-reporting pattern. Flags describe the
/// current interface, not every operation supported by its underlying backend.
/// Transparent layers forward them; transforming layers must update them to
/// match the operations they actually expose. Unset flags mean unsupported.
/// Construct values by explicitly assigning every flag or using a named constructor
/// instead of defaults or struct update syntax.
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
    /// The current interface implements concurrent execution of independent files
    /// or partitions. Implies `read`; independent of range access and output ordering.
    /// Reports an execution capability, not whether a particular request runs more
    /// than one partition at once (the plan or worker limit may allow only one).
    /// Merely exposing partitions through [`crate::ScanPlanner`] does not imply it.
    /// Inspecting this flag does not enable execution. It does not describe
    /// per-reader byte I/O concurrency or caller-managed concurrency across readers.
    pub read_parallel: bool,
    /// Can discover object references through [`crate::ByteAccessor::list`].
    /// Always false for readers and interfaces that do not expose discovery.
    pub list: bool,
}

/// Semantic permission to relax the logical order of a record scan.
/// Sources with unresolved semantic constraints must explicitly choose global order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderRequirement {
    Reorderable,
    PreserveGlobal,
}

impl OrderRequirement {
    /// Composition may strengthen an ordering requirement, never weaken it.
    pub fn combine(self, other: Self) -> Self {
        if self == Self::PreserveGlobal || other == Self::PreserveGlobal {
            Self::PreserveGlobal
        } else {
            Self::Reorderable
        }
    }
}

/// Record operations and the source's semantic ordering requirement.
/// No default is provided: every record adapter must declare or forward ordering.
/// `ordering` is permission to relax source order, not the selected emission policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecordCapability {
    pub read: bool,
    /// Always false for record interfaces.
    pub read_with_range: bool,
    pub read_parallel: bool,
    /// Always false for record interfaces.
    pub list: bool,
    /// Requirement covering every input accepted by this record interface.
    pub ordering: OrderRequirement,
}

impl Capability {
    /// Declare sequential reading support with no range, parallel or list access.
    /// Pass the input's read support when adapting an existing interface.
    pub const fn sequential(read: bool) -> Self {
        Self {
            read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    /// Declare the ordering contract while constructing a record capability.
    pub const fn with_ordering(self, ordering: OrderRequirement) -> RecordCapability {
        RecordCapability {
            read: self.read,
            read_with_range: self.read_with_range,
            read_parallel: self.read_parallel,
            list: self.list,
            ordering,
        }
    }
}

impl RecordCapability {
    /// Declare sequential record reading support with an explicit ordering contract.
    pub const fn sequential_if(read: bool, ordering: OrderRequirement) -> Self {
        Capability::sequential(read).with_ordering(ordering)
    }

    /// Capabilities of an opened sequential record stream, with explicit ordering.
    pub const fn sequential(ordering: OrderRequirement) -> Self {
        Self::sequential_if(true, ordering)
    }
}
