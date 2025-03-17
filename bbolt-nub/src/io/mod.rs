use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::pages::bytes::LazyPage;
use crate::pages::freelist::FreelistPage;
use crate::pages::meta::MetaPage;
use crate::pages::{Page, PageBytes};
use error_stack::Result;

pub mod disk_cache;

//AsRef<[u8]>

pub trait ReadData: Sized {
  type Output: PageBytes;

  fn read_data(&self, disk_page_id: DiskPageId) -> Result<Self::Output, DiskReadError>;
}

pub trait ReadPage: ReadData {
  type PageOutput: PageBytes;

  fn read_meta(
    &self, meta_page_id: MetaPageId,
  ) -> Result<MetaPage<Self::PageOutput>, DiskReadError>;

  fn read_freelist(
    &self, freelist_page_id: FreelistPageId,
  ) -> Result<FreelistPage<Self::PageOutput>, DiskReadError>;

  fn read_node(&self, node_page_id: NodePageId) -> Result<Page<Self::PageOutput>, DiskReadError>;
}

pub trait ReadOverflow: ReadPage<PageOutput = LazyPage<<Self as ReadData>::Output, Self>> {
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> Result<Self::Output, DiskReadError>;
  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> Result<Self::Output, DiskReadError>;
}
