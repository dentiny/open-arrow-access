# Project Gutenberg book metadata

## Service and data

Project Gutenberg distributes public-domain ebooks and per-book RDF/XML
catalogue metadata. This provider converts one RDF document into one Arrow book
metadata row, including authors, subjects and published file links.

Enable facade feature `service-gutenberg`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`GutenbergAccessor::with_default_decoder(storage).open(ReadRequest)` selects one
RDF object relative to caller-configured storage. It returns a `RecordReader` with
one row and this fixed schema (`?` means nullable):

```text
ebook_id: UInt64
title, rights: LargeUtf8?
creator: List<Struct<uri: Utf8?, name: Utf8?, alias: List<Utf8>, birthdate: Int32?, deathdate: Int32?>>
language, bookshelf: List<Utf8>
subject: List<Struct<scheme: Utf8?, value: Utf8>>
issued: Date32?
files: List<Struct<uri: Utf8, format: List<Utf8>, extent: UInt64?, modified: Utf8?>>
```

Missing repeated values become empty lists; birth/death dates are years and
`issued` is an Arrow date. File entries keep RDF order. URLs are retained as
metadata, without downloading books or verifying extents. Record source is the
RDF locator. Projection/order and byte options use standard `ReadRequest`
settings; known mapped fields remain validated even when projected away.

## Usage and results

```rust
use open_arrow_access::{
    Accessor, ReadRequest, Result,
    service::gutenberg::GutenbergAccessor,
    storage::{OpenDalAccessor, opendal},
};

async fn read_book_metadata(operator: opendal::Operator, rdf_key: &str) -> Result<()> {
    let books = GutenbergAccessor::with_default_decoder(OpenDalAccessor::new(operator));
    let mut reader = books.open(ReadRequest::new(rdf_key)).await?;
    if let Some(batch) = reader.next().await? {
        println!(
            "rows: {}, title: {:?}",
            batch.num_rows(),
            batch.column_by_name("title")
        );
        println!("available book files: {:?}", batch.column_by_name("files"));
    }
    Ok(())
}
```

Given one catalogue RDF object, the example obtains a metadata row and prints
its title/file columns. Ebook text is a separate download selected from those
links; there is no `files[].data` column.

Source: [Project Gutenberg machine-readable catalogue](https://www.gutenberg.org/ebooks/offline_catalogs.html).
