use super::{PmcInput, PmcRequest, capability, invalid, reader::PmcReader, schema, valid_path};
use futures::{TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, Capability, ObjectRef, ObjectStream, RecordMetadata, RecordReader,
    Result, async_trait,
};
use std::sync::Arc;

/// Read PMC PDF snapshots using an existing byte accessor.
///
/// Supply a bucket-rooted OpenDAL accessor or an FsAccessor rooted at a mirror.
/// Backend setup, credentials and byte layers remain caller-controlled. No I/O
/// starts until `open`; article contents are fetched only as `next` is polled.
pub struct PmcAccessor<A> {
    inner: Arc<A>,
}

impl<A> Clone for PmcAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> PmcAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[async_trait]
impl<A: ByteAccessor + 'static> Accessor for PmcAccessor<A> {
    type Request = PmcRequest;
    type Reader = dyn RecordReader;

    fn capability(&self) -> Capability {
        capability(self.inner.capability().read)
    }

    async fn open(&self, request: PmcRequest) -> Result<Box<dyn RecordReader>> {
        let (schema, projection) = request.options.resolve_projection(&schema())?;
        let objects: ObjectStream = match request.input {
            PmcInput::Files(objects) => {
                if objects.iter().any(|object| !valid_path(object.locator())) {
                    return Err(invalid(
                        "metadata objects must be bucket-relative paths without dot components",
                    ));
                }
                Box::pin(stream::iter(objects.into_iter().map(Ok)))
            }
            PmcInput::Prefix(mut list_request) => {
                if !self.inner.capability().list {
                    return Err(open_arrow_access_core::Error::new(
                        open_arrow_access_core::ErrorKind::Unsupported,
                        "open_pmc",
                        "byte accessor does not support listing; use explicit metadata files",
                    ));
                }
                let prefix = list_request.prefix.locator();
                if prefix == "/" {
                    return Err(invalid("expected a bucket-relative directory prefix"));
                }
                let prefix = prefix.strip_suffix('/').unwrap_or(prefix);
                if !prefix.is_empty() && !valid_path(prefix) {
                    return Err(invalid("expected a bucket-relative directory prefix"));
                }
                let prefix = if prefix.is_empty() {
                    String::new()
                } else {
                    format!("{prefix}/")
                };
                list_request.prefix = ObjectRef::new(prefix.clone());
                let recursive = list_request.recursive;
                Box::pin(
                    self.inner
                        .list(list_request)
                        .await?
                        .try_filter_map(move |object| {
                            let selected = (|| {
                                let relative =
                                    object.locator().strip_prefix(&prefix).ok_or_else(|| {
                                        invalid("PMC listing returned an object outside its prefix")
                                    })?;
                                if relative.is_empty() || relative.ends_with('/') {
                                    return Ok(None);
                                }
                                if !valid_path(relative) || (!recursive && relative.contains('/')) {
                                    return Err(invalid(
                                        "PMC listing returned an invalid object path",
                                    ));
                                }
                                Ok(relative.ends_with(".json").then_some(object))
                            })();
                            futures::future::ready(selected)
                        }),
                )
            }
        };
        Ok(Box::new(PmcReader {
            accessor: self.inner.clone(),
            objects: Some(objects),
            options: request.options,
            byte_options: request.byte_options,
            limits: request.limits,
            projection,
            metadata: RecordMetadata {
                schema,
                source: None,
            },
        }))
    }
}
