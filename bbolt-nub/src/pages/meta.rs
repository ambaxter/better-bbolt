use crate::common::bucket::BucketHeader;
use crate::common::id::{EOFPageId, FreelistPageId, TxId};
use crate::common::page::PageHeader;
use crate::pages::bytes::HasRootPage;
use crate::pages::{HasHeader, Page, PageBytes};
use bytemuck::{Pod, Zeroable};
use delegate::delegate;

/// `Meta` represents the on-file layout of a database's metadata
///
/// `meta` in Go BBolt
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Pod, Zeroable)]
pub struct Meta {
  /// Uniquely ID for BBolt databases
  pub magic: u32,
  /// Database version number
  pub version: u32,
  /// Database page size where page address = [PageId] * meta.page_size
  pub page_size: u32,
  pub flags: u32,
  /// Root bucket header
  pub root: BucketHeader,
  /// FreeList page location
  pub free_list: FreelistPageId,
  /// The end of the database where EOF = meta.eof_id * meta.page_size
  pub eof_id: EOFPageId,
  /// Current transaction ID
  pub tx_id: TxId,
  /// Checksum of the previous Meta fields using the 64-bit version of the Fowler-Noll-Vo hash function
  pub checksum: u64,
}

pub trait HasMeta: HasHeader {
  fn meta(&self) -> &Meta;
}

#[derive(Clone)]
pub struct MetaPage<T: PageBytes> {
  page: Page<T>,
}

impl<T: PageBytes> HasRootPage for MetaPage<T> {
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<T: PageBytes> HasHeader for MetaPage<T> {
  delegate! {
      to &self.page {
          fn page_header(&self) -> &PageHeader;
      }
  }
}
