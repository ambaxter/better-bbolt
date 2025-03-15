use crate::common::id::DiskPageId;
use crate::io::ReadPageData;
use moka::sync::Cache;

pub struct DiskCache<R: ReadPageData> {
  r: R,
  root_cache: Cache<DiskPageId, R::RootDataBytes>,
  page_cache: Cache<DiskPageId, R::DataBytes>,
}
