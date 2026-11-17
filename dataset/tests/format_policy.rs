use open_arrow_access_core::{ErrorKind, ObjectRef};
use open_arrow_access_dataset::auto_format::{DetectedFormat::*, FormatPolicy, detect_format};

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
        ("dir.with.dot/records.GZ", Some(Json)),
        ("archive.tar.gz", None),
        ("archive.TGZ", None),
        ("README.txt", None),
        ("a.unknown.gz", None),
    ] {
        assert_eq!(detect_format(name), expected, "{name}");
    }
}

#[test]
fn policy_selects_exclusive_and_compatible_files_or_reports_examples() {
    let policy = FormatPolicy::default();
    let selected = objects(&["metadata.json", "b.parquet", "a.parquet", "b.parquet"]);
    assert_eq!(
        policy.select(&selected).unwrap(),
        (Parquet, selected[1..].to_vec())
    );
    for (names, format) in [(["a.csv", "b.tsv"], Csv), (["b.tsv", "a.csv"], Tsv)] {
        let input = objects(&names);
        assert_eq!(policy.select(&input).unwrap(), (format, input));
    }
    for names in [&["a.csv", "b.tsv", "c.jsonl"][..], &["a.warc", "c.jsonl"]] {
        let error = policy.select(&objects(names)).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains(names[0]) && error.to_string().contains("c.jsonl"));
    }
    assert_eq!(
        policy.select(&objects(&["README.txt"])).unwrap_err().kind(),
        ErrorKind::NotFound
    );
    let strict = FormatPolicy {
        exclusive: vec![],
        allow_mixed_csv_tsv: false,
        ..Default::default()
    };
    for names in [["a.csv", "b.parquet"], ["a.csv", "b.tsv"]] {
        assert_eq!(
            strict.select(&objects(&names)).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    let csv_only = FormatPolicy {
        allowed: vec![Csv, Tsv],
        ..Default::default()
    };
    assert_eq!(
        csv_only
            .select(&objects(&["a.csv", "b.parquet"]))
            .unwrap()
            .1,
        objects(&["a.csv"])
    );
}

#[test]
fn preferences_follow_policy_order_after_allowed_filtering() {
    let input = objects(&["a.parquet", "b.json", "c.csv", "d.json"]);
    let mut policy = FormatPolicy {
        exclusive: vec![Json, Parquet],
        ..Default::default()
    };
    assert_eq!(
        policy.select(&input).unwrap(),
        (Json, objects(&["b.json", "d.json"]))
    );
    policy.allowed = vec![Parquet, Csv];
    assert_eq!(
        policy.select(&input).unwrap(),
        (Parquet, objects(&["a.parquet"]))
    );
}
