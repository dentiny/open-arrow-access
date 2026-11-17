use open_arrow_access_core::{Accessor, Capability, Reader, Result, StreamReader, async_trait};
use open_arrow_access_test_support::{DropProbe, assert_metadata_matches_open};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
struct DefaultAccessor(Arc<AtomicBool>);
#[async_trait]
impl Accessor for DefaultAccessor {
    type Request = ();
    type Reader = dyn Reader<Item = u8, Metadata = String, Capability = Capability>;
    fn capability(&self) -> Capability {
        Capability::default()
    }
    async fn open(&self, _: ()) -> Result<Box<Self::Reader>> {
        let probe = DropProbe::new(self.0.clone());
        let stream = futures::stream::once(async move {
            let _probe = probe;
            panic!("metadata polled an output item");
            #[allow(unreachable_code)]
            Ok(1)
        });
        Ok(Box::new(StreamReader::new(stream, "metadata".to_owned())))
    }
}
#[tokio::test]
async fn compatibility_default_clones_metadata_and_drops_reader() {
    let dropped = Arc::new(AtomicBool::new(false));
    let accessor = Arc::new(DefaultAccessor(dropped.clone()));
    assert_eq!(accessor.metadata(()).await.unwrap(), "metadata");
    assert!(dropped.load(Ordering::SeqCst));
    assert_metadata_matches_open(&accessor, ()).await;
}
