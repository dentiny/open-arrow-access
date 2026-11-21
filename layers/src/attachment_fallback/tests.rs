use super::*;
use open_arrow_access_core::AttachmentDataType::{Latex, Pdf, Txt};

struct Source(fn(u32) -> Result<Option<AttachmentCell>>);

#[async_trait]
impl AttachmentCellAccessor for Source {
    type Request = u32;

    async fn read_cell(&self, request: u32) -> Result<Option<AttachmentCell>> {
        (self.0)(request)
    }
}

fn error(kind: ErrorKind) -> Error {
    Error::new(kind, "read_attachment", "source failure")
}

#[tokio::test]
async fn each_cell_prefers_latex_and_only_missing_cells_fetch_pdf() {
    let cells = AttachmentFallbackLayer::new(Source(|id| {
        assert_eq!(id, 2); // The first cell must never fetch its PDF.
        Ok(Some(
            AttachmentCell::new(Pdf, b"%PDF-1.7\xff".as_slice()).with_source("paper.pdf"),
        ))
    }))
    .wrap(Source(|id| {
        Ok((id == 1).then(|| AttachmentCell::new(Latex, b"latex".as_slice())))
    }));
    assert_eq!(
        cells.read_cell(1).await.unwrap(),
        Some(AttachmentCell::new(Latex, b"latex".as_slice()))
    );
    assert_eq!(
        cells.read_cell(2).await.unwrap(),
        Some(AttachmentCell::new(Pdf, b"%PDF-1.7\xff".as_slice()).with_source("paper.pdf"))
    );
}

#[tokio::test]
async fn empty_content_exists_and_two_missing_cells_return_none() {
    let cells = AttachmentFallbackLayer::new(Source(|id| {
        assert_eq!(id, 2);
        Ok(None)
    }))
    .wrap(Source(|id| {
        Ok((id == 1).then(|| AttachmentCell::new(Txt, b"".as_slice())))
    }));
    assert_eq!(
        cells.read_cell(1).await.unwrap(),
        Some(AttachmentCell::new(Txt, b"".as_slice()))
    );
    assert_eq!(cells.read_cell(2).await.unwrap(), None);
}

#[tokio::test]
async fn fatal_errors_propagate_and_not_found_selects_fallback() {
    let cells = AttachmentFallbackLayer::new(Source(|id| {
        assert_eq!(id, 2); // Decode failure must not select another attachment.
        Err(error(ErrorKind::PermissionDenied))
    }))
    .wrap(Source(|id| {
        Err(error(if id == 1 {
            ErrorKind::Decode
        } else {
            ErrorKind::NotFound
        }))
    }));
    assert_eq!(
        cells.read_cell(1).await.unwrap_err().kind(),
        ErrorKind::Decode
    );
    assert_eq!(
        cells.read_cell(2).await.unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn custom_conditions_and_chained_candidates_select_one_cell() {
    let alternate = AttachmentFallbackLayer::new(Source(|_| {
        Ok(Some(AttachmentCell::new(Pdf, b"pdf".as_slice())))
    }))
    .wrap(Source(|_| Ok(None)));
    let cells = AttachmentFallbackLayer::new(alternate)
        .when(|e| e.kind() == ErrorKind::Unsupported)
        .wrap(Source(|_| Err(error(ErrorKind::Unsupported))));
    assert_eq!(
        cells.read_cell(1).await.unwrap(),
        Some(AttachmentCell::new(Pdf, b"pdf".as_slice()))
    );
}
