# Enhancement layers

The public `Layer<A>` and `AccessorExt` contracts are in `core/`. The initial
PR validates them with test-only wrappers; it exposes no production layer.

The required first functional layer is GeneratedColumnsLayer, described in
`docs/generated-columns.md`; expression evaluation belongs to a follow-up PR.
The framework includes `ReadOptionsProvider` for generic record-request settings.
MetricsLayer can follow separately. Later execution and emission
layers remain independent: ParallelReadLayer controls concurrency and
UnorderedEmissionLayer relaxes output ordering. Both stay default-off and use
the common accessor/reader protocol with optional planning extensions.

The [source-attribute proposal](../docs/source-attributes.md) defines policy
admission: ATIF requires global order and rejects UnorderedEmissionLayer.
Planning-aware layers also wrap the partition accessor returned by the planner
so that partition execution retains generated columns, observation and budgets.
