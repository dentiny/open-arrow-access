//! CourtListener bulk table discovery and decoded CSV byte access.
//! Storage is rooted above `bulk-data/`; schemas and record conversion remain
//! with the consumer. See the service README for public S3 configuration.

use std::sync::Arc;

use open_arrow_access_core::{
    Accessor, ByteAccessor, ByteReadRequest, ByteReader, Capability, Error, ErrorKind, ListRequest,
    ObjectRef, ReaderFactoryExt, Result, async_trait, list_files,
};
use open_arrow_access_decompression::{Bzip2, DecompressionFactory};

/// One table export and its UTC generation date, not an atomic database snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CourtListenerSnapshot {
    table: String,
    date: String,
}

impl CourtListenerSnapshot {
    /// Table filename name, e.g. `opinions`, `opinion-clusters`, or `courts`.
    pub fn new(table: impl Into<String>, date: impl Into<String>) -> Result<Self> {
        let table = table.into();
        let date = date.into();
        if !valid_table(&table) || !valid_date(&date) {
            return Err(invalid());
        }
        Ok(Self { table, date })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn date(&self) -> &str {
        &self.date
    }

    pub fn object(&self) -> ObjectRef {
        format!("bulk-data/{}-{}.csv.bz2", self.table, self.date).into()
    }

    /// Companion SQL object; never fetched or executed by this service.
    pub fn schema_object(&self) -> ObjectRef {
        format!("bulk-data/schema-{}.sql", self.date).into()
    }
}

/// Discover snapshots and stream original CSV bytes through the existing Bzip2
/// decoder. Input storage supplies compressed objects beneath `bulk-data/`.
pub struct CourtListenerBulkAccessor<A> {
    inner: Arc<A>,
}

impl<A> Clone for CourtListenerBulkAccessor<A> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<A> CourtListenerBulkAccessor<A> {
    pub fn new(inner: A) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<A: ByteAccessor> CourtListenerBulkAccessor<A> {
    /// Sorted, unique exports of one table. Collects names, not table contents.
    /// Requires listing; explicit reads do not. An empty match returns an empty vector.
    pub async fn snapshots(&self, table: &str) -> Result<Vec<CourtListenerSnapshot>> {
        if !valid_table(table) {
            return Err(invalid());
        }
        let prefix = format!("{table}-");
        let objects = list_files(
            self.inner.as_ref(),
            ListRequest::new("bulk-data/").with_recursive(false),
            |name| Ok(name.starts_with(&prefix) && name.ends_with(".csv.bz2")),
        )
        .await?;
        Ok(objects
            .into_iter()
            .filter_map(|object| {
                let (name, date) = parse_object(object.locator())?;
                (name == table).then(|| CourtListenerSnapshot {
                    table: name.into(),
                    date: date.into(),
                })
            })
            .collect())
    }
}

#[async_trait]
impl<A: ByteAccessor> Accessor for CourtListenerBulkAccessor<A> {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;

    fn capability(&self) -> Capability {
        Capability {
            read: self.inner.capability().read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    async fn open(&self, request: ByteReadRequest) -> Result<Box<dyn ByteReader>> {
        parse_object(request.object.locator()).ok_or_else(invalid)?;
        DecompressionFactory::new(Bzip2)
            .open_reader(self.inner.as_ref(), request)
            .await
    }
}

#[async_trait]
impl<A: ByteAccessor> ByteAccessor for CourtListenerBulkAccessor<A> {}

fn parse_object(locator: &str) -> Option<(&str, &str)> {
    let stem = locator
        .strip_prefix("bulk-data/")?
        .strip_suffix(".csv.bz2")?;
    let (table, date) = stem.split_at_checked(stem.len().checked_sub(11)?)?;
    let date = date.strip_prefix('-')?;
    (valid_table(table) && valid_date(date)).then_some((table, date))
}

fn valid_table(table: &str) -> bool {
    table.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && table
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

fn valid_date(date: &str) -> bool {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || !bytes
            .iter()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
    {
        return false;
    }
    let year: u32 = date[..4].parse().unwrap();
    let month: u32 = date[5..7].parse().unwrap();
    let day: u32 = date[8..].parse().unwrap();
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = match month {
        2 => 28 + u32::from(leap),
        4 | 6 | 9 | 11 => 30,
        1..=12 => 31,
        _ => 0,
    };
    year > 0 && day > 0 && day <= days
}

fn invalid() -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        "courtlistener_snapshot",
        "expected a lowercase table name and a valid YYYY-MM-DD date (bulk-data/<table>-<date>.csv.bz2)",
    )
}
