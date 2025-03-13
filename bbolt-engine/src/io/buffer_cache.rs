use moka::sync::Cache;
use crate::backend::file::BufferCacheEntry;
use crate::common::ids::DiskPageId;

pub struct BufferCache {
  cache: Cache<DiskPageId, BufferCacheEntry>,
}