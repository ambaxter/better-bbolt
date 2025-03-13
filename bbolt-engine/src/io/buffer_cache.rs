use crate::backend::file::BufferCacheEntry;
use crate::common::ids::DiskPageId;
use moka::sync::Cache;

pub struct BufferCache {
  cache: Cache<DiskPageId, BufferCacheEntry>,
}
