# Enhancement layers

The public `Layer<A>` and `AccessorExt` contracts are in `core/`. The initial
PR validates them with test-only wrappers; it exposes no production layer.

The first proposed implementation is MetricsLayer. Later execution and emission
layers remain independent: ParallelReadLayer controls concurrency and
UnorderedEmissionLayer relaxes output ordering. Both stay default-off and use
the common accessor/reader protocol with optional planning extensions.
