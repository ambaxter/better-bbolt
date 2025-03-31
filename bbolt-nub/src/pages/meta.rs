use crate::common::bucket::BucketHeader;
use crate::common::id::{EOFPageId, FreelistPageId, TxId};
use crate::common::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, TxPage};
use crate::pages::Page;
use bytemuck::{Pod, Zeroable};
use delegate::delegate;
use fnv_rs::{Fnv64, FnvHasher};
use std::hash::Hasher;

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

impl Meta {
  pub fn sum64(&self) -> u64 {
    let mut h = Fnv64::new();
    let bytes = &bytemuck::bytes_of(self)[0..size_of::<Meta>() - size_of::<u64>()];
    h.update(bytes);
    h.finish()
  }

  pub fn is_valid(&self) -> bool {
    let checksum = self.sum64();
    checksum == self.checksum
  }

  pub fn update_checksum(&mut self) {
    self.checksum = self.sum64();
  }
}

#[repr(C)]
#[derive(Debug, Default, Copy, Clone, Pod, Zeroable)]
pub struct HeaderMetaPage {
  pub(crate) header: PageHeader,
  pub(crate) meta: Meta,
}

pub trait HasMeta: HasHeader {
  fn meta(&self) -> &Meta;
}

#[derive(Clone)]
pub struct MetaPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for MetaPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}
