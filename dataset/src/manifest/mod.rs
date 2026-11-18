//! Streaming paths manifests over caller-bound storage. No network clients or
//! credentials are created here. The storage root must correspond to the origin.
mod error;
mod validation;
use crate::file_set::FileSetRequest;
use futures::StreamExt;
use open_arrow_access_core::{
    ByteAccessor, ByteReadOptions, ByteReadRequest, ByteReader, ObjectRef, ReaderFactoryExt,
    Result, read_lines,
};
use open_arrow_access_decompression::CompressionFactory;
pub use open_arrow_access_file_format::{DetectedFormat, detect_format};
use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub enum ManifestBase {
    #[default]
    Origin,
    ManifestDirectory,
    /// A directory, relative to the storage root (trailing slash optional).
    Explicit(ObjectRef),
}
/// How to handle entries that do not match the selected file formats.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Mismatch {
    /// Stop with an error on the first format mismatch.
    #[default]
    Error,
    /// Skip entries whose formats do not match.
    Skip,
}
/// How to handle duplicate selected files.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Duplicates {
    /// Stop with an error on the first duplicate.
    #[default]
    Error,
    /// Keep only the first occurrence, preserving manifest order.
    Dedupe,
}

/// Resolve a manifest into ordered object keys. Defaults reject duplicates,
/// URL query/fragment, credentials, traversal and cross-origin URLs.
/// Relative manifest keys require `with_origin` to accept absolute URL entries.
/// Memory holds selected keys and the duplicate set, never the whole manifest.
#[derive(Clone, Debug)]
pub struct PathsManifest {
    manifest: ObjectRef,
    base: ManifestBase,
    prefix: Option<String>,
    formats: Option<(Vec<DetectedFormat>, Mismatch)>,
    duplicates: Duplicates,
    origin: Option<String>,
    byte_options: ByteReadOptions,
}
impl PathsManifest {
    pub fn new(manifest: impl Into<ObjectRef>) -> Self {
        Self {
            manifest: manifest.into(),
            base: ManifestBase::Origin,
            prefix: None,
            formats: None,
            duplicates: Duplicates::Error,
            origin: None,
            byte_options: ByteReadOptions::default(),
        }
    }
    pub fn with_base(mut self, base: ManifestBase) -> Self {
        self.base = base;
        self
    }
    /// Filter resolved origin-relative keys, before format and duplicate checks.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }
    pub fn with_formats(mut self, formats: &[DetectedFormat], on_mismatch: Mismatch) -> Self {
        self.formats = Some((formats.to_vec(), on_mismatch));
        self
    }
    pub fn with_duplicates(mut self, duplicates: Duplicates) -> Self {
        self.duplicates = duplicates;
        self
    }
    /// HTTP(S) origin of a relative manifest key. Validated before any I/O.
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.origin = Some(origin.into());
        self
    }
    pub fn with_byte_options(mut self, options: ByteReadOptions) -> Self {
        self.byte_options = options;
        self
    }

    /// Open raw manifest bytes and decompress by suffix using enabled codecs.
    /// Enable codecs through open-arrow-access-decompression (or facade features).
    /// The supplied storage must not already decompress the manifest.
    pub async fn resolve<B: ByteAccessor>(&self, storage: &B) -> Result<FileSetRequest> {
        let context = self.context()?;
        let request =
            ByteReadRequest::new(context.manifest_key.clone()).with_options(self.byte_options);
        let input = CompressionFactory::default()
            .open_reader(storage, request)
            .await
            .map_err(|e| self.open_error(e))?;
        self.collect(input, context).await
    }
    /// Resolve already-decoded bytes, for existing compositions that own the
    /// decompression layer. The same validation and selection policies apply.
    pub async fn resolve_reader(&self, input: Box<dyn ByteReader>) -> Result<FileSetRequest> {
        self.collect(input, self.context()?).await
    }
    async fn collect(
        &self,
        input: Box<dyn ByteReader>,
        context: validation::Context,
    ) -> Result<FileSetRequest> {
        let lines = read_lines(input, None, "read_paths_manifest").enumerate();
        futures::pin_mut!(lines);
        let mut objects = Vec::new();
        let mut seen = HashSet::new();
        let mut number = 1;
        while let Some((index, line)) = lines.next().await {
            number = index + 1;
            let line = line.map_err(|e| self.read_error(number, e))?;
            let Some(path) = self.selected_path(&line, number, &context)? else {
                continue;
            };
            if !seen.insert(path.clone()) {
                if self.duplicates == Duplicates::Dedupe {
                    continue;
                }
                return Err(self.duplicate_file(number));
            }
            objects.push(ObjectRef::new(path));
        }
        if objects.is_empty() {
            return Err(self.empty_selection(number));
        }
        Ok(FileSetRequest::new(objects).with_byte_options(self.byte_options))
    }
}
