# Independent API page scans

`common::PageSetAccessor` binds an application-supplied page opener to an ordered
selection of independently fetchable API requests. The opener handles provider
validation, authentication and Arrow decoding. `ParallelReadLayer` runs the
resulting partitions within one resident-partition and prefetch budget.

## Runnable example

The [read_api_pages example](../examples/read_api_pages.rs) composes
`Connection::json`, `ApiDocument::records_at`, an explicit `JsonFormat`, and
`PageSetAccessor`. It only needs the facade's `format-json` feature:

```sh
cargo run --no-default-features --features format-json --example read_api_pages -- \
  https://api.example.org records /items 2 1 2
```

Use an actual API origin/path that accepts an independent `page` query parameter
and returns an `items` array with unsigned integer `id` fields, for example:

```json
{"items": [{"id": 42}], "next": "/records?page=3"}
```

The example requests pages `[2, 1, 2]`, retaining the duplicate and output order.
It reads the selected `/items` array using a fixed `id: UInt64` schema. Adjust
the format and pointer for your API; the `next` field is not followed. For a
provider with semantic validation, call that provider's page method inside the
opener instead of the generic `Connection::json` method.

Configure credentials, custom transport and HTTP policies on the connection
before capturing it. `Connection` applies origin-scoped authentication to each
page request; do not implement a second credential/redirect mechanism in the
callback. Provider-specific constructors remain preferable when available.

## Reuse and execution

`PageSetAccessor::clone()` shares the opener through `Arc`; the callback, request
and future types do not need `Clone`. Use `pages.clone().layer(...)` to configure
one scan while retaining the same page opener for later selections. Each `open`
owns its selection and reader cursors, and opened readers can outlive the
accessor. Any mutable state captured by the callback remains shared; cloning
does not create a source snapshot or isolate application caches.

Construction and cloning perform no page requests. Opening/planning fetches the
first selected page to establish the fixed schema. Remaining pages are opened
lazily by the selected executor. An empty selection is invalid. Use one explicit
schema across pages; later schema drift terminates the scan with a decode error.

The example permits two resident page partitions with one queued batch per
partition. Later pages may fetch/decode concurrently, but output retains request
order even when a later page finishes first. Completed pages with queued batches
still occupy budget slots. Prefetch bounds batch counts, not total bytes; set
format batch sizes and HTTP timeouts separately. Page sets require global order
and do not permit unordered emission. See [parallel reading](../docs/parallel-read.md).

## Cursor-dependent pagination

`PageSetAccessor` does not discover continuation URLs or tokens, and does not
assume cursor requests are independent. Discover dependent next-page descriptors
in protocol order first. Only use a page set once the selected requests can be
issued independently under the API's rules. It does not automatically harvest
every page, follow a `next` field or retry the same selection as a snapshot.
