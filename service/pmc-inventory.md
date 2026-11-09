# PMC inventory snapshots

## Service and data

PMC inventory snapshots describe article-metadata objects in the
`pmc-oa-opendata` bucket. A JSON manifest lists gzip CSV shards. This provider
reads those inventory rows, not the article metadata or full-text objects.

Enable facade feature `service-pmc`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

Construct `PmcInventoryAccessor::new(storage)` and open a `ReadRequest` selecting
the exact inventory manifest locator. Storage must resolve both that locator and
the manifest's shard keys; the library preserves keys without rewriting roots.

The manifest declares `sourceBucket`, `destinationBucket`, `version`,
`creationTimestamp`, `fileFormat`, `fileSchema` and `files[].key`. Supported
inventories use CSV shards ending in `.csv.gz` and require `Bucket`, `Key`,
`LastModifiedDate`, `ETag` columns. Every output column is nullable Utf8, including
numeric-looking fields. Extra declared columns and CSV values are retained.

The response is an ordered `RecordReader`, with batching/projection from
`ReadOptions`. Bucket/Key validation still runs under an empty projection. `Key`
is not URL-decoded; an inventory value is not automatically an article request.
Record source is the manifest locator. Schema metadata contains
`pmc.inventory.manifest_uri`, `pmc.inventory.creation_timestamp` and
`pmc.inventory.version`. Empty
inventories keep their schema and yield EOF. Shards are opened lazily in order.

Illustrative upstream inventory manifest shape (values are examples;
additional fields may be present):

```json
{
  "sourceBucket": "pmc-oa-opendata",
  "destinationBucket": "arn:aws:s3:::pmc-oa-opendata",
  "version": "2016-11-30",
  "creationTimestamp": "1762041600000",
  "fileFormat": "CSV",
  "fileSchema": "Bucket,Key,LastModifiedDate,ETag",
  "files": [
    {
      "key": "inventory/data/part-000.csv.gz"
    }
  ]
}
```

`creationTimestamp` is a decimal string of Unix epoch milliseconds; `fileSchema`
is a comma-separated string, not an Arrow schema JSON object.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    service::pmc_inventory::PmcInventoryAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read_inventory(operator: opendal::Operator, manifest_key: &str) -> Result<()> {
    let inventory = PmcInventoryAccessor::new(OpenDalAccessor::new(operator));
    let mut reader = inventory.open(ReadRequest::new(manifest_key)).await?;
    println!("schema: {:?}", reader.metadata().schema);
    if let Some(batch) = reader.next().await? {
        println!(
            "inventory rows: {}, Key column: {:?}",
            batch.num_rows(),
            batch.column_by_name("Key")
        );
    }
    Ok(())
}
```

The example prints the declared schema and first inventory batch's Key column.
Keys identify source objects for a later selection step; no article URLs are opened.

Sources: [PMC data](https://pmc-oa-opendata.s3.amazonaws.com/README.txt), [S3 inventory manifests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-inventory-location.html).
