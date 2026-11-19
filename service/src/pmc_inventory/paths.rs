//! Turn inventory values into bucket-relative article requests, decoding keys once.
use super::{PmcInventoryAccessor, error, validation};
use arrow_array::cast::AsArray;
use futures::{StreamExt, TryStreamExt, stream};
use open_arrow_access_core::{
    Accessor, ByteAccessor, ObjectRef, ObjectStream, ReadRequest, Result, into_stream,
};

impl<A: ByteAccessor + 'static> PmcInventoryAccessor<A> {
    /// Stream decoded metadata keys, preserving duplicate inventory records.
    /// Inventory columns stay internal; caller projection applies to articles.
    pub async fn metadata_paths(&self, mut request: ReadRequest) -> Result<ObjectStream> {
        request.options.projection = Some(vec!["Key".into()]);
        let reader = self.open(request).await?;
        Ok(into_stream(reader)
            .map_ok(|batch| {
                // InventoryTransform has already rejected null Bucket/Key values.
                let keys = batch.column(0).as_string::<i32>();
                let objects = keys.iter().flatten().map(metadata_key).collect::<Vec<_>>();
                stream::iter(objects)
            })
            .try_flatten()
            .boxed())
    }
}

fn metadata_key(value: &str) -> Result<ObjectRef> {
    let key = percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map_err(error::key_encoding)?;
    validation::validate_metadata_key(&key)?;
    Ok(key.into_owned().into())
}
