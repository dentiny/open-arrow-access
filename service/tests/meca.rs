use open_arrow_access_core::{Accessor, ByteReadRequest, ErrorKind};
use open_arrow_access_decompression::zip::ZipLimits;
use open_arrow_access_service::meca::MecaArchive;
use open_arrow_access_test_support::{MemoryAccessor, collect_bytes, fixture};
use std::num::NonZeroUsize;
async fn open(name: &str) -> open_arrow_access_core::Result<MecaArchive> {
    let data = fixture!("meca", name).await;
    let input = MemoryAccessor::new(&data, 7)
        .with_ranges()
        .open(ByteReadRequest::new("bundle.meca"))
        .await?;
    MecaArchive::open(input, ZipLimits::default()).await
}
#[tokio::test]
async fn original_manifest_and_independent_members_are_retained() {
    let archive = open("valid.meca").await.unwrap();
    assert!(archive.manifest().starts_with(b"<?xml"));
    assert_eq!(archive.files().len(), 2);
    assert_eq!(archive.files()[1].href, "content/paper%20one.pdf");
    assert_eq!(archive.archive().entries().len(), 4);
    let mut pdf = archive
        .archive()
        .open_entry(
            archive.files()[1].entry_index,
            NonZeroUsize::new(2).unwrap(),
        )
        .await
        .unwrap();
    drop(archive);
    assert_eq!(collect_bytes(pdf.as_mut()).await.unwrap(), b"%PDF-test");
}
#[tokio::test]
async fn missing_ambiguous_and_unsafe_references_fail() {
    for name in [
        "missing.meca",
        "unsafe.meca",
        "duplicate.meca",
        "wrong-root.meca",
    ] {
        assert_eq!(
            open(name).await.err().unwrap().kind(),
            ErrorKind::Decode,
            "{name}"
        );
    }
}
