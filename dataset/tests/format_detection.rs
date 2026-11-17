use open_arrow_access_core::{ErrorKind, ObjectRef};
use open_arrow_access_dataset::auto_format::{DetectedFormat::*, detect_format, select_format};

fn objects(names: &[&str]) -> Vec<ObjectRef> {
    names.iter().map(|name| ObjectRef::from(*name)).collect()
}

#[test]
fn detects_suffixes_without_reader_or_codec_features() {
    for (name, expected) in [
        ("a.PARQUET?x=.csv", Some(Parquet)),
        ("a.CSV.GZ?token=x", Some(Csv)),
        ("a.TSV.zstd#fragment", Some(Tsv)),
        ("a.NDJSON.BZ2", Some(Json)),
        ("a.jsonl.LZ4", Some(Json)),
        ("a.JSON", Some(Json)),
        ("a.WARC.GZ", Some(Warc)),
        ("dir.with.dot/records.GZ", None),
        ("archive.tar.gz", None),
        ("archive.TGZ", None),
        ("records.csv.tgz", None),
        ("records.tsv.TGZ", None),
        ("records", None),
        ("records.bz2", None),
        ("records.lz4", None),
        ("records.zst", None),
        ("records.jsonl.zip", None),
        ("", None),
        ("README.txt", None),
        ("a.unknown.gz", None),
    ] {
        assert_eq!(detect_format(name), expected, "{name}");
    }
}

#[test]
fn selection_preserves_order_duplicates_and_original_locators() {
    let input = objects(&[
        "README.txt",
        "b.CSV.GZ?token=x",
        "a.csv",
        "b.CSV.GZ?token=x",
    ]);
    assert_eq!(select_format(&input).unwrap(), (Csv, input[1..].to_vec()));
    for names in [
        vec!["a.json", "b.jsonl", "c.ndjson.gz"],
        vec!["a.tsv", "b.tsv.gz"],
    ] {
        let input = objects(&names);
        assert_eq!(select_format(&input).unwrap().1, input);
    }
}

#[test]
fn selection_rejects_mixed_formats_and_reports_examples() {
    for names in [
        ["a.csv", "b.tsv"],
        ["b.tsv", "a.csv"],
        ["a.csv", "b.parquet"],
        ["b.parquet", "a.csv"],
        ["a.parquet", "metadata.json"],
        ["a.warc", "b.jsonl"],
    ] {
        let error = select_format(&objects(&names)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(names.iter().all(|name| error.to_string().contains(name)));
    }
    for names in [
        vec![],
        vec!["README.txt"],
        vec!["records.gz", "archive.tgz"],
    ] {
        assert_eq!(
            select_format(&objects(&names)).unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }
}
