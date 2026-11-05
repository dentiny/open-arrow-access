# WARC

Enable the default-off `format-warc` feature to read a single logical WARC input
through any shared byte accessor. `WarcAccessor::new(bytes)` implements the same
`Accessor<Request = ReadRequest, Reader = dyn RecordReader>` contract as Parquet;
it needs sequential bytes, not ranged storage. Projection, batch size and
per-open byte I/O options use the existing request fields.

The adapter reuses the asynchronous parser from
[dentiny/warc-rs](https://github.com/dentiny/warc-rs), with no custom framing or
parser task. The immutable Git dependency includes
[the optional-storage change](https://github.com/dentiny/warc-rs/pull/28), which
lets callers use their own asynchronous streams without introducing a second
OpenDAL version. The existing Lance-aligned Arrow/Parquet/OpenDAL pins stay
unchanged. Replace the Git pin with a compatible published release once available.

## Representation

`warc_schema()` exposes the existing ml-infra reader's 20 columns, in the same
order, with the same types and nullability. The first four fields are `id: Utf8`,
`content_length: UInt64`, `date: Timestamp(Millisecond, None)`, and `type: Utf8`, all
non-null. Optional WARC headers follow; `body: BinaryView NOT NULL` is last.
Date is normalized by the upstream parser to UTC milliseconds. The body contains
the entire WARC content block, including embedded HTTP headers where present;
there is no HTTP-payload extraction, text decoding or response-only filter.

Every physical record is emitted in input order, including `warcinfo`, `request`,
`revisit` and `continuation` records. Continuation records retain their segment
headers and remain separate rows. A Common Crawl segment is a dataset of WARC
files, not a different WARC physical record format; its resolver belongs in a
separate provider PR.

Required headers are validated by warc-rs. Optional numeric segment fields are
validated when selected. Optional text header decoding follows warc-rs's lossy
UTF-8 policy. Unknown extension headers are not added to the fixed schema. The
parser's accepted WARC versions/header syntax apply; this adapter is not a
complete archival-conformance validator or digest verifier.

## Streaming and memory

Opening forwards storage options and returns fixed schema metadata without
parsing records. Pulling a batch reads physical records through the upstream
async iterator, validates bodies and terminators, and stops at the requested row
target or the body-byte target. `with_batch_body_bytes` defaults to 16 MiB; one
larger record can exceed it. EOF and errors are terminal, and dropping the reader
leaves no detached producer running.

Each record body is buffered once by the parser and transferred into Arrow's
BinaryView buffers without copying into another body buffer. BinaryView values
must be smaller than `u32::MAX` bytes. Metadata/projection may reduce retained
Arrow columns, but the current upstream raw-record iterator still reads and
buffers each full body, including when `body` is not projected. The body-byte
and row targets are not total memory bounds or maximum record sizes.

External compression is explicit: place a decompression byte layer before the
WARC accessor once the corresponding feature PR is available. `format-warc`
does not infer compression from a filename or enable a hidden gzip dependency.
This PR adds no index, parallel record scan, generated columns or sink.
