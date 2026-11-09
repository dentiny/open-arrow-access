use crate::Accessor;

/// A type-preserving enhancement of an accessor and the readers it creates.
///
/// Byte layers and record layers use this same trait. A layer may wrap `open`
/// and each reader operation; it must delegate optional operations or report the
/// changed capabilities. Decoding Bytes into RecordBatch is an adapter, not a
/// type-preserving layer. Type preservation refers to Rust interfaces, not Arrow
/// fields: a record layer may change schema and values together, exposing the final
/// schema before the first batch. Execution/emission layers can later specialize this
/// contract with optional planning extensions before opening readers.
pub trait Layer<A: Accessor> {
    /// Decorated accessor retaining the inner accessor's public request/reader types.
    type Layered: Accessor<Request = A::Request, Reader = A::Reader>;

    /// Wrap `inner` to construct the enhanced accessor without starting a read.
    ///
    /// The returned accessor can intercept opening and wrap the readers it creates.
    /// It must preserve the shared metadata/error contracts and delegate optional
    /// operations or accurately report their changed capabilities. Any intended
    /// transformation of byte values or metadata must be documented by that layer.
    fn layer(&self, inner: A) -> Self::Layered;
}

/// Convenience methods for composing layers around an owned accessor.
pub trait AccessorExt: Accessor + Sized {
    /// Consume this accessor and install `layer` around it.
    ///
    /// Returns the layer's decorated accessor. In `accessor.layer(a).layer(b)`,
    /// `b` is outermost and sees opening before delegating to `a`. Construction
    /// itself does not open a reader or start consuming input.
    fn layer<L: Layer<Self>>(self, layer: L) -> L::Layered {
        layer.layer(self)
    }
}

impl<A: Accessor> AccessorExt for A {}
