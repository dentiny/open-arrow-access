//! Resolve content types within JSON independently of file-format selection.
//!
//! Inputs must already be selected as JSON, and storage must supply decoded bytes.
//! This resolver distinguishes ordinary JSON records from ATIF framings and checks
//! consistency across files when `EveryFile` is selected. It does not detect suffixes
//! or choose compression codecs. Reader features control decoding, not type identity.

mod error;
mod sniff;
mod validation;

use crate::file_set::FileSetRequest;
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    ByteAccessor, Error, ObjectRef, RecordScanAccessor, Result, SchemaRef,
};
use open_arrow_access_file_format::AtifFormat;
use std::sync::Arc;

pub use sniff::{SniffConfig, SniffPolicy};

/// Content type within the JSON file format, independent of enabled readers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonKind {
    /// Ordinary JSON records handled by the native JSON reader.
    Records,
    Atif(AtifFormat),
}

/// Selected inputs and their resolved JSON content type.
/// With `FirstFile`, inputs after the first nonempty file remain unchecked.
#[derive(Clone, Debug)]
pub struct ResolvedJsonFormat {
    pub kind: JsonKind,
    pub objects: Vec<ObjectRef>,
}

/// Configurable resolver for JSON content types and their native readers.
#[derive(Clone, Debug, Default)]
pub struct JsonResolver {
    schema: Option<SchemaRef>,
    sniff: SniffConfig,
}

impl JsonResolver {
    pub fn with_schema_opt(mut self, schema: Option<SchemaRef>) -> Self {
        self.schema = schema;
        self
    }

    pub fn with_sniff_config(mut self, config: SniffConfig) -> Self {
        self.sniff = config;
        self
    }

    /// Probe decoded bytes without consuming the readers used for record execution.
    /// By default, skip empty inputs until the first nonempty file and trust its
    /// kind for all later inputs. `EveryFile` checks all files, skips every empty
    /// input, and rejects mixed content kinds. Both policies preserve order and duplicates.
    pub async fn resolve(
        &self,
        storage: Arc<dyn ByteAccessor>,
        request: &FileSetRequest,
    ) -> Result<ResolvedJsonFormat> {
        let concurrency = match self.sniff.policy {
            SniffPolicy::EveryFile => self.sniff.concurrency.get(),
            SniffPolicy::FirstFile => 1,
        };
        let probes = stream::iter(request.objects.iter().cloned().enumerate())
            .map(|(index, object)| {
                let storage = storage.clone();
                async move {
                    let kind = sniff::probe(
                        storage.as_ref(),
                        &object,
                        request.byte_options,
                        self.sniff.max_bytes.get(),
                    )
                    .await?;
                    Ok::<_, Error>(kind.map(|kind| (index, object, kind)))
                }
            })
            .buffer_unordered(concurrency)
            .try_filter_map(|sample| futures::future::ready(Ok(sample)));
        futures::pin_mut!(probes);
        let Some((index, example, kind)) = probes.try_next().await? else {
            return Err(error::empty_selection(request.objects.first()));
        };
        validation::validate_schema(self.schema.as_ref(), kind, Some(&example))?;
        if self.sniff.policy == SniffPolicy::FirstFile {
            let mut objects = vec![example];
            objects.extend_from_slice(&request.objects[index + 1..]);
            return Ok(ResolvedJsonFormat { kind, objects });
        }
        let mut selected = vec![(index, example.clone())];
        while let Some((index, object, other)) = probes.try_next().await? {
            validation::validate_same_kind(kind, &example, other, &object)?;
            selected.push((index, object));
        }
        selected.sort_unstable_by_key(|(index, _)| *index);
        let objects = selected.into_iter().map(|(_, object)| object).collect();
        Ok(ResolvedJsonFormat { kind, objects })
    }

    /// Bind the selected native reader to decoded storage. No record I/O occurs here.
    pub fn reader(
        &self,
        storage: Arc<dyn ByteAccessor>,
        kind: JsonKind,
        object: Option<&ObjectRef>,
    ) -> Result<Arc<dyn RecordScanAccessor>> {
        validation::validate_schema(self.schema.as_ref(), kind, object)?;
        match (kind, storage) {
            #[cfg(feature = "format-json")]
            (JsonKind::Records, storage) => {
                use open_arrow_access_file_format::json::JsonAccessor;
                Ok(Arc::new(match &self.schema {
                    Some(schema) => JsonAccessor::new(storage, schema.clone()),
                    None => JsonAccessor::infer(storage),
                }))
            }
            #[cfg(feature = "format-atif")]
            (JsonKind::Atif(format), storage) => Ok(Arc::new(
                open_arrow_access_file_format::atif::AtifAccessor::new(storage, format),
            )),
            #[allow(unreachable_patterns)]
            (kind, _) => Err(error::disabled_reader(kind, object)),
        }
    }
}
