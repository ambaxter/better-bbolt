use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::pages::bytes::{LazyPage, TxPage};
use crate::pages::freelist::FreelistPage;
use crate::pages::meta::MetaPage;
use crate::pages::{Page, PageBytes};
use error_stack::Result;

pub mod disk_cache;

//AsRef<[u8]>

pub trait ReadData: Sized {
  type Output<'tx>: TxPage<'tx>
  where
    Self: 'tx;

  fn read_data<'tx>(
    &'tx self, disk_page_id: DiskPageId,
  ) -> Result<Self::Output<'tx>, DiskReadError>;
}

pub trait ReadPage: ReadData {
  type PageOutput<'tx>: TxPage<'tx>
  where
    Self: 'tx;

  fn read_meta<'tx>(
    &'tx self, meta_page_id: MetaPageId,
  ) -> Result<MetaPage<Self::PageOutput<'tx>>, DiskReadError>;

  fn read_freelist<'tx>(
    &'tx self, freelist_page_id: FreelistPageId,
  ) -> Result<FreelistPage<Self::PageOutput<'tx>>, DiskReadError>;

  fn read_node<'tx>(
    &'tx self, node_page_id: NodePageId,
  ) -> Result<Page<Self::PageOutput<'tx>>, DiskReadError>;
}

pub trait ReadOverflow: ReadPage {
  fn read_freelist_overflow<'tx>(
    &'tx self, root_page_id: FreelistPageId, overflow: u32,
  ) -> Result<Self::Output<'tx>, DiskReadError>;
  fn read_node_overflow<'tx>(
    &'tx self, root_page_id: NodePageId, overflow: u32,
  ) -> Result<Self::Output<'tx>, DiskReadError>;
}
