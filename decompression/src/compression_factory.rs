use std::{borrow::Cow, collections::BTreeMap, fmt, num::NonZeroUsize, sync::Arc};

use open_arrow_access_core::{
    ByteReadRequest, ByteReader, Capability, Error, ErrorKind, FactoryToken, ReaderFactory,
    ReaderFactoryExt, Result, async_trait,
};

use crate::{Codec, CompressedInput, Compression, DecodedInput, DecompressionFactory};

#[derive(Clone)]
struct RegisteredCodec {
    name: &'static str,
    decoder: Arc<dyn Fn(CompressedInput) -> DecodedInput + Send + Sync>,
}

impl Codec for RegisteredCodec {
    fn name(&self) -> &'static str {
        self.name
    }

    fn decoder(&self, input: CompressedInput) -> DecodedInput {
        (self.decoder)(input)
    }
}

#[derive(Clone)]
struct Registration {
    name: &'static str,
    codec: Option<RegisteredCodec>,
    builtin: Option<Compression>,
    decoded_extension: Option<&'static str>,
}

struct RegisteredExtension<'a> {
    extension: &'a str,
    registration: &'a Registration,
}

impl Registration {
    fn new<C: Codec>(codec: C, builtin: Option<Compression>) -> Self {
        Self {
            name: codec.name(),
            codec: Some(RegisteredCodec {
                name: codec.name(),
                decoder: Arc::new(move |input| codec.decoder(input)),
            }),
            builtin,
            decoded_extension: None,
        }
    }

    fn codec(&self) -> Result<RegisteredCodec> {
        self.codec
            .clone()
            .ok_or_else(|| crate::error::disabled_codec(self.name))
    }
}

/// Register streaming codecs by extension and create per-object byte readers.
/// Defaults include built-in suffixes, even when their codec features are disabled.
/// Clones retain the same registrations; later registration changes are local.
/// Share a configured clone with format readers that inspect compression suffixes.
#[derive(Clone)]
pub struct CompressionFactory {
    // Maps lowercase extensions without a leading dot to codec registrations.
    registrations: Arc<BTreeMap<String, Registration>>,
    chunk_size: NonZeroUsize,
}

impl fmt::Debug for CompressionFactory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompressionFactory")
            .field("extensions", &self.registrations.keys().collect::<Vec<_>>())
            .field("chunk_size", &self.chunk_size)
            .finish()
    }
}

impl Default for CompressionFactory {
    fn default() -> Self {
        let mut factory = Self {
            registrations: Arc::new(BTreeMap::new()),
            chunk_size: crate::layer::DEFAULT_CHUNK_SIZE,
        };
        macro_rules! builtin {
            ($feature:literal, $variant:ident, $codec:expr, [$($suffix:literal),+]) => {{
                #[cfg(feature = $feature)]
                let registration = Registration::new($codec, Some(Compression::$variant));
                #[cfg(not(feature = $feature))]
                let registration = Registration { name: $feature, codec: None, builtin: None, decoded_extension: None };
                $(Arc::make_mut(&mut factory.registrations).insert($suffix.into(), registration.clone());)+
            }};
        }
        builtin!("gzip", Gzip, crate::gzip::Gzip, ["gz"]);
        let mut tar_gzip = factory.registrations["gz"].clone();
        tar_gzip.decoded_extension = Some("tar");
        Arc::make_mut(&mut factory.registrations).insert("tgz".into(), tar_gzip);
        builtin!("bz2", Bzip2, crate::bz2::Bzip2, ["bz2"]);
        builtin!("lz4", Lz4, crate::lz4::Lz4, ["lz4"]);
        builtin!("zstd", Zstd, crate::zstd::Zstd, ["zst", "zstd"]);
        factory
    }
}

impl CompressionFactory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or replace one extension (with or without a leading dot).
    /// Matching is case-insensitive. Extensions must be single filename suffixes.
    pub fn register<C: Codec>(&mut self, extension: &str, codec: C) -> Result<()> {
        let extension = extension.strip_prefix('.').unwrap_or(extension);
        if extension.is_empty() || extension.contains(['.', '/', '\\', '?', '#']) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "register_compression",
                "expected one nonempty filename extension",
            ));
        }
        Arc::make_mut(&mut self.registrations).insert(
            extension.to_ascii_lowercase(),
            Registration::new(codec, None),
        );
        Ok(())
    }

    /// All registered extensions, including recognized but disabled built-in codecs.
    pub fn extensions(&self) -> impl Iterator<Item = &str> {
        self.registrations.keys().map(String::as_str)
    }

    /// Remove one registered suffix without decoding or interpreting the locator as a URL.
    /// Recognition is independent of whether a built-in codec feature is enabled.
    /// Combined suffixes retain the decoded format: `.tgz` becomes `.tar`.
    pub fn strip_suffix<'a>(&self, path: &'a str) -> Cow<'a, str> {
        match path.rsplit_once('.') {
            Some((stem, extension)) => match self.lookup(extension) {
                Some(RegisteredExtension { registration, .. }) => {
                    match registration.decoded_extension {
                        Some(extension) => Cow::Owned(format!("{stem}.{extension}")),
                        None => Cow::Borrowed(stem),
                    }
                }
                None => Cow::Borrowed(path),
            },
            None => Cow::Borrowed(path),
        }
    }

    pub fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    fn lookup(&self, extension: &str) -> Option<RegisteredExtension<'_>> {
        self.registrations
            .get_key_value(&extension.to_ascii_lowercase())
            .map(|(extension, registration)| RegisteredExtension {
                extension,
                registration,
            })
    }

    pub(crate) fn builtin_from_path(&self, path: &str) -> Result<Compression> {
        let Some((_, extension)) = path.rsplit_once('.') else {
            return Ok(Compression::Uncompressed);
        };
        let Some(RegisteredExtension { registration, .. }) = self.lookup(extension) else {
            return Ok(Compression::Uncompressed);
        };
        registration.codec()?;
        Ok(registration.builtin.unwrap_or(Compression::Uncompressed))
    }
}

#[async_trait]
impl ReaderFactory for CompressionFactory {
    type Request = ByteReadRequest;
    type Reader = dyn ByteReader;
    type Prepared = Option<String>;

    fn capability(&self, input: Capability) -> Capability {
        Capability {
            read: input.read,
            read_with_range: false,
            read_parallel: false,
            list: false,
        }
    }

    fn prepare(&self, request: &ByteReadRequest) -> Result<Self::Prepared> {
        let Some((_, extension)) = request.object.locator().rsplit_once('.') else {
            return Ok(None);
        };
        let Some(RegisteredExtension {
            extension,
            registration,
        }) = self.lookup(extension)
        else {
            return Ok(None);
        };
        registration.codec()?;
        Ok(Some(extension.to_owned()))
    }

    async fn create_reader(
        &self,
        _token: &FactoryToken,
        input: Box<dyn ByteReader>,
        request: ByteReadRequest,
        prepared: Self::Prepared,
    ) -> Result<Box<dyn ByteReader>> {
        let Some(extension) = prepared else {
            return Ok(input);
        };
        let registration = self.registrations.get(&extension).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "auto_decompress",
                "unregistered compression extension",
            )
        })?;
        DecompressionFactory::new(registration.codec()?)
            .with_chunk_size(self.chunk_size)
            .open_input(input, request)
            .await
    }
}
