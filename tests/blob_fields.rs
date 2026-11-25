use arrow_schema::{DataType, Fields, SchemaRef};
use open_arrow_access::{
    BLOB_METADATA_KEY, BLOB_METADATA_VALUE, BinaryDecoder, DocumentDecoder, ReadRequest,
    archive_schema,
};

fn blob_paths(fields: &Fields, prefix: &str, paths: &mut Vec<String>) {
    for field in fields {
        let path = if prefix.is_empty() {
            field.name().clone()
        } else {
            format!("{prefix}.{}", field.name())
        };
        if field.metadata().get(BLOB_METADATA_KEY).map(String::as_str) == Some(BLOB_METADATA_VALUE)
        {
            paths.push(path);
        } else {
            let mut ty = field.data_type();
            while let DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _) = ty
            {
                ty = item.data_type();
            }
            if let DataType::Struct(children) = ty {
                blob_paths(children, &path, paths);
            }
        }
    }
}

#[test]
fn source_schemas_mark_only_payloads_and_selected_downloads() {
    let mut cases: Vec<(&str, SchemaRef, Vec<&str>)> = vec![
        (
            "binary",
            BinaryDecoder.prepare(&ReadRequest::new("file")).unwrap(),
            vec!["data"],
        ),
        ("archive", archive_schema(), vec!["data"]),
    ];
    #[cfg(feature = "service-arxiv")]
    cases.extend([
        (
            "arxiv articles",
            open_arrow_access::service::arxiv::article_schema(),
            vec!["data"],
        ),
        (
            "arxiv bulk",
            open_arrow_access::service::arxiv::record_schema(),
            vec!["data"],
        ),
        (
            "arxiv content",
            open_arrow_access::service::arxiv::content_schema(),
            vec!["data"],
        ),
    ]);
    #[cfg(feature = "service-bhl")]
    cases.push((
        "bhl",
        open_arrow_access::service::bhl::page_schema(),
        vec!["data"],
    ));
    #[cfg(feature = "service-meca")]
    cases.push((
        "meca",
        open_arrow_access::service::meca::record_schema(),
        vec!["data"],
    ));
    #[cfg(feature = "service-gutenberg")]
    cases.extend([
        (
            "gutenberg books",
            open_arrow_access::service::gutenberg::book_schema(),
            vec!["files.data", "raw_rdf"],
        ),
        (
            "gutenberg metadata",
            open_arrow_access::service::gutenberg::schema(),
            vec![],
        ),
    ]);
    #[cfg(feature = "service-pmc")]
    cases.push((
        "pmc",
        open_arrow_access::service::pmc::schema(),
        vec!["content_url"],
    ));
    #[cfg(feature = "service-courtlistener")]
    cases.extend([
        (
            "courtlistener metadata",
            open_arrow_access::service::courtlistener::CourtListenerSchemas::default().schema(),
            vec![],
        ),
        (
            "courtlistener documents",
            open_arrow_access::service::courtlistener::CourtListenerSchemas::default()
                .schema_with_document_urls(),
            vec!["documents.opinion_file_url", "documents.cluster_pdf_url"],
        ),
    ]);
    #[cfg(feature = "format-warc")]
    cases.push((
        "warc",
        open_arrow_access::file_format::warc::arrow_schema(),
        vec!["body"],
    ));
    for (name, schema, expected) in cases {
        let mut paths = vec![];
        blob_paths(schema.fields(), "", &mut paths);
        assert_eq!(paths, expected, "{name}");
        let projected = schema
            .project(&(0..schema.fields().len()).rev().collect::<Vec<_>>())
            .unwrap();
        let mut projected_paths = vec![];
        blob_paths(projected.fields(), "", &mut projected_paths);
        paths.sort();
        projected_paths.sort();
        assert_eq!(projected_paths, paths, "{name} projection");
    }
}
