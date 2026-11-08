use open_arrow_access_core::{Error, ErrorKind, ObjectRef, Result};

/// One table export, identified by its published UTC generation date.
/// Equal dates across tables do not imply an atomic database snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CourtListenerSnapshot {
    table: String,
    date: String,
}

impl CourtListenerSnapshot {
    /// Use a filename table name, e.g. `opinions`, `opinion-clusters`, or `courts`.
    /// Names are not a closed enum so newly published tables remain accessible.
    pub fn new(table: impl Into<String>, date: impl Into<String>) -> Result<Self> {
        let table = table.into();
        let date = date.into();
        validate_table(&table)?;
        if !valid_date(&date) {
            return Err(invalid("expected a valid UTC date in YYYY-MM-DD form"));
        }
        Ok(Self { table, date })
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn date(&self) -> &str {
        &self.date
    }

    /// Locator relative to the configured storage bucket or mirror root.
    pub fn object(&self) -> ObjectRef {
        format!("bulk-data/{}-{}.csv.bz2", self.table, self.date).into()
    }

    /// Companion SQL schema object; never fetched or executed by this service.
    pub fn schema_object(&self) -> ObjectRef {
        format!("bulk-data/schema-{}.sql", self.date).into()
    }

    pub(super) fn from_object(object: &ObjectRef) -> Result<Self> {
        let stem = object
            .locator()
            .strip_prefix("bulk-data/")
            .and_then(|name| name.strip_suffix(".csv.bz2"))
            .ok_or_else(|| invalid("expected bulk-data/<table>-YYYY-MM-DD.csv.bz2"))?;
        let split = stem
            .len()
            .checked_sub(11)
            .filter(|&i| stem.is_char_boundary(i) && stem.as_bytes().get(i) == Some(&b'-'));
        let Some(split) = split else {
            return Err(invalid("expected <table>-YYYY-MM-DD.csv.bz2"));
        };
        Self::new(&stem[..split], &stem[split + 1..])
    }
}

pub(super) fn validate_table(table: &str) -> Result<()> {
    if table.is_empty()
        || !table.as_bytes()[0].is_ascii_lowercase()
        || !table
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return Err(invalid("expected a lowercase bulk table filename name"));
    }
    Ok(())
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
    let month: usize = date[5..7].parse().unwrap();
    let day: u32 = date[8..].parse().unwrap();
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    year > 0 && (1..=12).contains(&month) && day > 0 && day <= days[month - 1]
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidInput, "courtlistener_snapshot", message)
}
