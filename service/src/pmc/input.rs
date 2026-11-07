use super::validation;
use open_arrow_access_core::{
    Accessor, ByteReadOptions, ByteReadRequest, ByteReader, ObjectRef, Result,
};

pub(super) async fn read_bounded<A>(
    accessor: &A,
    object: ObjectRef,
    options: ByteReadOptions,
    limit: usize,
) -> Result<Vec<u8>>
where
    A: Accessor<Request = ByteReadRequest, Reader = dyn ByteReader>,
{
    let path = object.locator().to_owned();
    let mut reader = accessor
        .open(ByteReadRequest::new(object).with_options(options))
        .await?;
    validation::validate_content_length(reader.metadata().content_length, limit, &path)?;
    let mut output = Vec::new();
    while let Some(bytes) = reader.next().await? {
        validation::validate_chunk(&bytes, limit - output.len(), limit, &path)?;
        output.extend_from_slice(&bytes);
    }
    Ok(output)
}
