use arrow_array::BooleanArray;
use arrow_select::filter::filter_record_batch;
use open_arrow_access_core::{
    Accessor, Error, ErrorKind, Layer, Reader, RecordBatch, RecordCapability, RecordMetadata,
    RecordReader, Result, async_trait,
};

/// Seeded Bernoulli sampling: independently retain each row with `probability`.
///
/// The count is approximate, not an exact-size or reservoir sample. For the same
/// ordered input and seed, row selection is independent of batch boundaries,
/// projection, column values and ordered parallel partition scheduling. Unordered
/// input may produce a different sample. Each open starts a fresh random stream.
/// Selected rows retain their input order and schema.
///
/// ```
/// use open_arrow_access_core::{Accessor, AccessorExt, RecordReader, Result};
/// use open_arrow_access_layers::{SamplingAccessor, SamplingLayer};
///
/// fn sample<A: Accessor<Reader = dyn RecordReader>>(source: A) -> Result<SamplingAccessor<A>> {
///     Ok(source.layer(SamplingLayer::try_new(0.1, 42)?))
/// }
/// ```
#[derive(Clone, Copy, Debug)]
pub struct SamplingLayer {
    threshold: u64,
    seed: u64,
}

const RANDOM_BITS: u32 = 53;
const SCALE: u64 = 1 << RANDOM_BITS;

impl SamplingLayer {
    /// Validate a finite probability in [0, 1] without opening a source.
    /// Probabilities use 53-bit resolution. Zero reads no source batches; one
    /// passes through the original reader. The source is opened for metadata.
    pub fn try_new(probability: f64, seed: u64) -> Result<Self> {
        crate::validation::validate_sampling_probability(probability)?;
        Ok(Self {
            threshold: (probability * SCALE as f64) as u64,
            seed,
        })
    }
}

/// Record accessor decorated by [`SamplingLayer`]. Put sampling outside a
/// `ParallelReadLayer` to use one random stream across the entire ordered scan.
/// It intentionally does not implement `ScanPlanner`: per-partition seed resets
/// would change selection with partition boundaries.
#[derive(Clone, Debug)]
pub struct SamplingAccessor<A> {
    inner: A,
    settings: SamplingLayer,
}

impl<A: Accessor<Reader = dyn RecordReader>> Layer<A> for SamplingLayer {
    type Layered = SamplingAccessor<A>;
    fn layer(&self, inner: A) -> Self::Layered {
        SamplingAccessor {
            inner,
            settings: *self,
        }
    }
}

#[async_trait]
impl<A: Accessor<Reader = dyn RecordReader>> Accessor for SamplingAccessor<A> {
    type Request = A::Request;
    type Reader = dyn RecordReader;

    fn capability(&self) -> RecordCapability {
        self.inner.capability()
    }

    async fn open(&self, request: Self::Request) -> Result<Box<dyn RecordReader>> {
        let inner = self.inner.open(request).await?;
        if self.settings.threshold == SCALE {
            return Ok(inner);
        }
        let metadata = inner.metadata().clone();
        let capability = inner.capability();
        Ok(Box::new(SamplingReader {
            inner: if self.settings.threshold == 0 {
                None
            } else {
                Some(inner)
            },
            metadata,
            capability,
            threshold: self.settings.threshold,
            random: SplitMix64(self.settings.seed),
        }))
    }
}

struct SamplingReader {
    inner: Option<Box<dyn RecordReader>>,
    metadata: RecordMetadata,
    capability: RecordCapability,
    threshold: u64,
    random: SplitMix64,
}

#[async_trait]
impl Reader for SamplingReader {
    type Item = RecordBatch;
    type Metadata = RecordMetadata;
    type Capability = RecordCapability;

    fn metadata(&self) -> &RecordMetadata {
        &self.metadata
    }
    fn capability(&self) -> RecordCapability {
        self.capability
    }

    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            let Some(inner) = self.inner.as_mut() else {
                return Ok(None);
            };
            let batch = match inner.next().await {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    self.inner = None;
                    return Ok(None);
                }
                Err(error) => {
                    self.inner = None;
                    return Err(error);
                }
            };
            // Exactly one draw per input row, including null and duplicate rows.
            let mask: BooleanArray = (0..batch.num_rows())
                .map(|_| self.random.next() >> (64 - RANDOM_BITS) < self.threshold)
                .collect();
            match filter_record_batch(&batch, &mask) {
                Ok(sampled) if sampled.num_rows() != 0 => return Ok(Some(sampled)),
                Ok(_) => {}
                Err(error) => {
                    self.inner = None;
                    return Err(Error::new(ErrorKind::Decode, "sample", error.to_string())
                        .with_source(error));
                }
            }
        }
    }
}

// SplitMix64's fixed wrapping arithmetic makes selection reproducible across
// platforms without depending on a thread-local RNG or external RNG defaults.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}
