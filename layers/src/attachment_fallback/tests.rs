use super::*;
use open_arrow_access_core::AttachmentDataType;
use std::{collections::HashMap, sync::Mutex};

#[derive(Clone)]
enum Outcome {
    Cell(AttachmentCell),
    Missing,
    Error(ErrorKind),
}

struct Source {
    outcomes: HashMap<u32, Outcome>,
    requests: Arc<Mutex<Vec<u32>>>,
}

impl Source {
    fn new(outcomes: impl IntoIterator<Item = (u32, Outcome)>) -> Self {
        Self {
            outcomes: outcomes.into_iter().collect(),
            requests: Arc::default(),
        }
    }
}

#[async_trait]
impl AttachmentCellAccessor for Source {
    type Request = u32;

    async fn read_cell(&self, request: u32) -> Result<Option<AttachmentCell>> {
        self.requests.lock().unwrap().push(request);
        match self.outcomes.get(&request).unwrap_or(&Outcome::Missing) {
            Outcome::Cell(cell) => Ok(Some(cell.clone())),
            Outcome::Missing => Ok(None),
            Outcome::Error(kind) => Err(Error::new(*kind, "read_attachment", "source failure")
                .with_object(Some(&format!("article/{request}").into()))),
        }
    }
}

#[tokio::test]
async fn rows_independently_keep_latex_or_fall_back_to_pdf() {
    let latex = AttachmentCell::new(
        AttachmentDataType::Latex,
        b"\\documentclass{article}".as_slice(),
    )
    .with_source("src/first.tex");
    let pdf = AttachmentCell::new(AttachmentDataType::Pdf, b"%PDF-1.7\xff".as_slice())
        .with_source("pdf/second.pdf");
    let primary = Source::new([(1, Outcome::Cell(latex.clone()))]);
    let fallback = Source::new([(2, Outcome::Cell(pdf.clone()))]);
    let primary_requests = primary.requests.clone();
    let fallback_requests = fallback.requests.clone();
    let cells = AttachmentFallbackLayer::new(fallback).wrap(primary);
    // Same accessor, different rows; no batch/stream selection is involved.
    let (first, second) = futures::join!(cells.read_cell(1), cells.read_cell(2));
    assert_eq!(first.unwrap(), Some(latex));
    assert_eq!(second.unwrap(), Some(pdf));
    let mut requested = primary_requests.lock().unwrap().clone();
    requested.sort();
    assert_eq!(requested, [1, 2]);
    assert_eq!(*fallback_requests.lock().unwrap(), [2]);
}

#[tokio::test]
async fn existing_empty_text_is_a_cell_not_absence() {
    let empty = AttachmentCell::new(AttachmentDataType::Txt, b"".as_slice());
    let primary = Source::new([(1, Outcome::Cell(empty.clone()))]);
    let fallback = Source::new([(1, Outcome::Error(ErrorKind::Internal))]);
    let requested = fallback.requests.clone();
    let cells = AttachmentFallbackLayer::new(fallback).wrap(primary);
    assert_eq!(cells.read_cell(1).await.unwrap(), Some(empty));
    assert!(requested.lock().unwrap().is_empty());
}

#[tokio::test]
async fn absent_cells_and_classified_not_found_try_only_the_alternate() {
    let primary = Source::new([(1, Outcome::Error(ErrorKind::NotFound))]);
    let fallback = Source::new([(
        1,
        Outcome::Cell(AttachmentCell::new(
            AttachmentDataType::Pdf,
            b"pdf".as_slice(),
        )),
    )]);
    let cells = AttachmentFallbackLayer::new(fallback).wrap(primary);
    assert_eq!(
        cells.read_cell(1).await.unwrap().unwrap().data_type,
        AttachmentDataType::Pdf
    );
    assert_eq!(cells.read_cell(2).await.unwrap(), None);
}

#[rstest::rstest]
#[case(ErrorKind::PermissionDenied)]
#[case(ErrorKind::Unauthenticated)]
#[case(ErrorKind::Timeout)]
#[case(ErrorKind::Decode)]
#[tokio::test]
async fn fatal_primary_errors_never_fetch_another_attachment(#[case] kind: ErrorKind) {
    let primary = Source::new([(1, Outcome::Error(kind))]);
    let fallback = Source::new([]);
    let requested = fallback.requests.clone();
    let cells = AttachmentFallbackLayer::new(fallback).wrap(primary);
    let error = cells.read_cell(1).await.unwrap_err();
    assert_eq!(error.kind(), kind);
    assert_eq!(error.operation(), "read_attachment");
    assert_eq!(error.object().unwrap().locator(), "article/1");
    assert!(requested.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fallback_errors_and_custom_type_tags_are_preserved() {
    let primary = Source::new([]);
    let custom = AttachmentCell::new(
        AttachmentDataType::Other("image/png".into()),
        b"png".as_slice(),
    );
    let fallback = Source::new([
        (1, Outcome::Error(ErrorKind::PermissionDenied)),
        (2, Outcome::Cell(custom.clone())),
    ]);
    let cells = AttachmentFallbackLayer::new(fallback).wrap(primary);
    let error = cells.read_cell(1).await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::PermissionDenied);
    assert_eq!(error.object().unwrap().locator(), "article/1");
    assert_eq!(cells.read_cell(2).await.unwrap(), Some(custom));
    assert_eq!(
        AttachmentDataType::Other("image/png".into()).as_str(),
        "image/png"
    );
}

#[tokio::test]
async fn chained_representations_and_explicit_conditions_still_select_one_cell() {
    let primary = Source::new([(1, Outcome::Error(ErrorKind::Unsupported))]);
    let xml = Source::new([]);
    let pdf = AttachmentCell::new(AttachmentDataType::Pdf, b"pdf".as_slice());
    let pdf_source = Source::new([(1, Outcome::Cell(pdf.clone()))]);
    let xml_or_pdf = AttachmentFallbackLayer::new(pdf_source).wrap(xml);
    let layer = AttachmentFallbackLayer::new(xml_or_pdf)
        .when(|error| error.kind() == ErrorKind::Unsupported);
    let cells = layer.clone().wrap(primary);
    assert_eq!(cells.read_cell(1).await.unwrap(), Some(pdf));
}
