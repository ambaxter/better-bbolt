use crate::common::id::{EOFPageId, FreelistPageId, TxId};
use crate::common::layout::bucket::BucketHeader;
use crate::common::layout::page::PageHeader;
use bytemuck::{Pod, Zeroable};
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
