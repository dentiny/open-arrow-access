use open_arrow_access_core::{Error, ErrorKind, Result};

/// cache_httpfs policy shared by a DuckDB database and its cloned connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    /// Cache remote blocks in memory, bounded by `block_size * max_blocks` bytes.
    InMemory { block_size: u64, max_blocks: u64 },
    /// Cache remote blocks in the connection-owned temporary directory.
    OnDisk,
}

impl Default for CachePolicy {
    /// A 128 MiB memory cache using 512 KiB blocks.
    fn default() -> Self {
        Self::InMemory {
            block_size: 524288,
            max_blocks: 256,
        }
    }
}

impl CachePolicy {
    pub(super) fn sql(self) -> Result<String> {
        let settings = match self {
            Self::InMemory {
                block_size,
                max_blocks,
            } => {
                if block_size == 0
                    || max_blocks == 0
                    || block_size.checked_mul(max_blocks).is_none()
                {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "duckdb_cache",
                        "cache block size and count must be positive with a capacity that fits u64",
                    ));
                }
                vec![
                    ("cache_httpfs_type", "'in_mem'".to_owned()),
                    ("cache_httpfs_cache_block_size", block_size.to_string()),
                    (
                        "cache_httpfs_max_in_mem_cache_block_count",
                        max_blocks.to_string(),
                    ),
                ]
            }
            Self::OnDisk => vec![("cache_httpfs_type", "'on_disk'".to_owned())],
        };
        Ok(settings
            .into_iter()
            .map(|(name, value)| format!("SET GLOBAL {name} = {value}; SET {name} = {value};"))
            .collect())
    }
}
