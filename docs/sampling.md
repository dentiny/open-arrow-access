# Seeded row sampling

`SamplingLayer` is a format-independent record layer, always available through
`open_arrow_access::layers`. It performs Bernoulli sampling: every input row is
independently retained with a configured probability. The number of returned rows
is approximate, not an exact-size sample or a reservoir sample.

```rust,ignore
use open_arrow_access::{Accessor, AccessorExt, layers::SamplingLayer};

let sampled = source.layer(SamplingLayer::try_new(0.10, 42)?);
let mut reader = sampled.open(request).await?;
while let Some(batch) = reader.next().await? {
    // Approximately 10% of input rows, in their original order.
}
```

The probability must be finite and in `[0, 1]`. The seed is a `u64`; configuration
validation performs no I/O. Each `open` starts a fresh random stream. Probability
zero opens the source for metadata, then drops its reader without requesting a
batch. Probability one returns the original reader, sharing all original buffers.

## Reproducibility

A fixed SplitMix64 stream produces one draw per input row. Its upper 53 bits are
compared with `floor(probability * 2^53)`, giving 53-bit probability resolution.
The arithmetic and mapping are fixed rather than relying on a thread-local RNG.
The layer keeps only RNG state and a Boolean selection mask for the current
batch; it never accumulates the dataset.

The same seed, probability and **ordered row sequence** yield the same selected
row positions, regardless of batch boundaries, empty batches, projection, column
types or row contents. Duplicate rows are independent observations. A different
seed normally gives a different sample; distinct seeds need not produce distinct
samples for small inputs. Reopening with the same seed restarts the sample.
Changing the input order can change the selected records. Unordered reads cannot
promise the same records across runs.

## Parallel reads and composition

Put sampling **outside** ordered parallel reading so the entire scan consumes one
random stream:

```rust,ignore
let sampled = source
    .layer(ParallelReadLayer::new(partitions, prefetch))
    .layer(SamplingLayer::try_new(0.10, 42)?);
```

Changing partition boundaries or worker count preserves selection when the inner
reader preserves the same row order. Sampling forwards the inner reader's
ordering and parallel-read capabilities. The wrapper itself does not implement
`ScanPlanner`: placing a parallel layer outside sampling is intentionally not
supported, since restarting the seed in each partition would change the sample.

The layer does not change the request or widen projection. Sampling after a
filter applies to the filtered row sequence; sampling before it applies to the
original sequence. A generated column or cast preserving row order/count does
not affect selected positions.

## Output and lifecycle

Schema, field/schema metadata, source identity, nulls and relative row order are
preserved. Zero-column batches retain the sampled row count. For probabilities
strictly between zero and one, batches with no selected rows are skipped. Output
batch sizes may vary and no re-batching is performed. Input is still decoded:
this reduces emitted rows, not source storage I/O.

There are no sampling tasks or prefetch queues. Source errors retain their error
category and source chain and terminate the reader. EOF/error releases the source
reader, and dropping the sampler drops its input, including any inner parallel
executor. After cancelling a pending read, drop the reader according to the shared
reader contract.
