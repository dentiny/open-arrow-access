# bioRxiv and medRxiv

## Service and data

bioRxiv and medRxiv host research preprints. This provider reads dated metadata
pages, preserves returned versions/licenses and resolves advertised JATS XML.
Bulk MECA bundles are a separate archive path over caller-configured storage.

Enable facade feature `service-biorxiv`. See [example setup](README.md#example-setup) for dependencies and the async runtime.

## Request and response

`details(server, from, until, cursor)` sends
`GET https://api.biorxiv.org/details/{server}/{from}/{until}/{cursor}/json`.
Dates use `YYYY-MM-DD`; `Server` selects Biorxiv or Medrxiv. Public metadata needs
no key. The response is `BiorxivPage { response: ApiDocument, next_cursor: Option<u64> }`.
`response.value()` contains upstream `messages` and `collection` records.

`jats(page)` performs no metadata request and returns `RemoteFiles<BiorxivPage>`.
Names are `paper-00000.xml`, etc., using each original collection row index and
its `jatsxml` URL. Missing links are omitted while page metadata remains.
Follow `next_cursor` explicitly; no automatic version deduplication occurs.
`Server::bulk_bucket()` gives the monthly bulk bucket name; configure storage and
use [MECA](meca.md) separately to inspect a bundle.

Illustrative upstream metadata page shape (values are examples;
additional fields may be present):

```json
{
  "messages": [
    {
      "status": "ok",
      "cursor": 0,
      "count": 1,
      "total": 2
    }
  ],
  "collection": [
    {
      "doi": "10.1101/example",
      "version": "1",
      "title": "Example preprint",
      "jatsxml": "https://files.example.org/paper.xml"
    }
  ]
}
```

## Usage and results

```rust
use futures::TryStreamExt;
use open_arrow_access::{
    Accessor, ByteAccessor, Result,
    service::biorxiv::{BiorxivService, Server},
};

async fn inspect_preprints(from: &str, until: &str) -> Result<()> {
    let service = BiorxivService::new()?;
    let page = service.details(Server::Biorxiv, from, until, 0).await?;
    println!("next cursor: {:?}", page.next_cursor);
    let files = service.jats(page)?;
    println!(
        "records: {}",
        files.metadata().response.value()["collection"]
    );
    let mut names = files.list("".into()).await?;
    if let Some(name) = names.try_next().await? {
        println!("file: {}", name.locator());
        let mut reader = files.open(name.into()).await?;
        if let Some(chunk) = reader.next().await? {
            println!("first chunk: {} bytes", chunk.len());
        }
    }
    Ok(())
}
```

The example prints a metadata page and its next cursor, then reads one JATS
file chunk if present. A chunk is XML bytes, not necessarily a complete article;
continue reading or apply an XML decoder to process its contents.

Sources: [metadata API](https://api.biorxiv.org/), [bioRxiv text/data mining](https://www.biorxiv.org/tdm).
