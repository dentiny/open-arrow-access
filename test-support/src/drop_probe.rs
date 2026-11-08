use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// Set a shared flag when the owning resource is dropped.
/// Capture the probe in a stream or reader to observe its resource lifetime.
pub struct DropProbe(Arc<AtomicBool>);

impl DropProbe {
    pub fn new(dropped: Arc<AtomicBool>) -> Self {
        Self(dropped)
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
