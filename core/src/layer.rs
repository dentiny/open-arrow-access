use crate::Accessor;

/// A type-preserving enhancement of an accessor and the readers it creates.
///
/// Byte layers and record layers use this same trait. A layer may wrap `open`
/// and each reader operation; it must delegate optional operations or report the
/// changed capabilities. Decoding Bytes into RecordBatch is an adapter, not a
/// type-preserving layer. Execution/emission layers can later specialize this
/// contract with optional planning extensions before opening readers.
pub trait Layer<A: Accessor> {
    type Layered: Accessor<Request = A::Request, Reader = A::Reader>;

    fn layer(&self, inner: A) -> Self::Layered;
}

pub trait AccessorExt: Accessor + Sized {
    /// Install a layer. The most recently added wrapper is outermost.
    fn layer<L: Layer<Self>>(self, layer: L) -> L::Layered {
        layer.layer(self)
    }
}

impl<A: Accessor> AccessorExt for A {}
