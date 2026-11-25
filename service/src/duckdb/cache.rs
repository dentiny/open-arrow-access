use open_arrow_access_core::{Error, ErrorKind, Result};

/// cache_httpfs policy shared by a DuckDB database and its cloned connections.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachePolicy {
    /// Cache remote blocks in memory, bounded by `block_size * max_blocks` bytes.
    InMemory { block_size: u64, max_blocks: u64 },
    /// Cache remote blocks on disk, with an optional memory read-through cache.
    OnDisk {
        block_size: u64,
        /// Number of memory blocks above the disk cache; zero disables this layer.
        max_in_memory_blocks: u64,
        /// Trigger disk eviction below this free-space threshold, in bytes.
        /// Zero retains cache_httpfs's default 5% free-space reservation.
        /// This is not a maximum cache-directory size.
        min_free_disk_bytes: u64,
    },
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
    /// Disk caching with the extension's default block size, 128 MiB memory
    /// read-through cache, and default free-space reservation.
    pub const fn on_disk() -> Self {
        Self::OnDisk {
            block_size: 524288,
            max_in_memory_blocks: 256,
            min_free_disk_bytes: 0,
        }
    }

    /// Validate block sizes and memory capacities before opening a connection.
    /// Disk mode permits zero memory blocks to disable its read-through cache.
    pub fn validate(&self) -> Result<()> {
        match *self {
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
            }
            Self::OnDisk {
                block_size,
                max_in_memory_blocks,
                ..
            } => {
                if block_size == 0 || block_size.checked_mul(max_in_memory_blocks).is_none() {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "duckdb_cache",
                        "disk cache block size must be positive and memory capacity must fit u64",
                    ));
                }
            }
        }
        Ok(())
    }

    pub(super) fn sql(self) -> Result<String> {
        self.validate()?;
        let settings = match self {
            Self::InMemory {
                block_size,
                max_blocks,
            } => {
                vec![
                    ("cache_httpfs_type", "'in_mem'".to_owned()),
                    ("cache_httpfs_cache_block_size", block_size.to_string()),
                    (
                        "cache_httpfs_max_in_mem_cache_block_count",
                        max_blocks.to_string(),
                    ),
                ]
            }
            Self::OnDisk {
                block_size,
                max_in_memory_blocks,
                min_free_disk_bytes,
            } => {
                let mut settings = vec![
                    ("cache_httpfs_type", "'on_disk'".to_owned()),
                    ("cache_httpfs_cache_block_size", block_size.to_string()),
                    (
                        "cache_httpfs_disk_cache_reader_enable_memory_cache",
                        (max_in_memory_blocks != 0).to_string(),
                    ),
                    (
                        "cache_httpfs_min_disk_bytes_for_cache",
                        min_free_disk_bytes.to_string(),
                    ),
                ];
                if max_in_memory_blocks != 0 {
                    settings.push((
                        "cache_httpfs_disk_cache_reader_mem_cache_block_count",
                        max_in_memory_blocks.to_string(),
                    ));
                }
                settings
            }
        };
        Ok(settings
            .into_iter()
            .map(|(name, value)| format!("SET GLOBAL {name} = {value}; SET {name} = {value};"))
            .collect())
    }
}
