use crate::backend::{MAGIC, VERSION};
use crate::common::bucket::BucketHeader;
use crate::common::buffer::PageBuffer;
use crate::common::ids::{BucketPageId, EOFPageId, FreelistPageId, PageId, TxId};
use crate::common::page::PageHeader;
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
  pub fn init(page_size: usize, tx_id: TxId) -> Meta {
    let mut meta = Meta {
      magic: MAGIC,
      version: VERSION,
      page_size: page_size as u32,
      flags: 0,
      root: BucketHeader::new(BucketPageId::of(3), 0),
      free_list: FreelistPageId::of(2),
      eof_id: EOFPageId::of(4),
      tx_id,
      checksum: 0,
    };
    let checksum = meta.sum64();
    meta.checksum = checksum;
    meta
  }
  pub fn sum64(&self) -> u64 {
    let mut h = Fnv64::new();
    let bytes = &bytemuck::bytes_of(self)[0..size_of::<Meta>() - size_of::<u64>()];
    h.update(bytes);
    h.finish()
  }

  pub fn update_checksum(&mut self) {
    self.checksum = self.sum64();
  }
}

pub struct MetaPage<'tx> {
  page: PageBuffer<'tx>,
}

impl<'tx> MetaPage<'tx> {
  pub fn new(page: PageBuffer<'tx>) -> Self {
    assert!(page.get_header().is_meta());
    Self { page }
  }

  pub fn get_meta(&self) -> &Meta {
    bytemuck::from_bytes(self.page.slice(size_of::<PageHeader>(), size_of::<Meta>()))
  }

  pub fn write(data: &mut [u8], page_header: &PageHeader, meta: &Meta) {
    assert!(page_header.is_meta());
    let header_bytes = bytemuck::bytes_of(page_header);
    let meta_start = header_bytes.len();
    data[0..meta_start].copy_from_slice(header_bytes);
    let meta_bytes = bytemuck::bytes_of(meta);
    data[meta_start..meta_start + meta_bytes.len()].copy_from_slice(meta_bytes);
  }

  delegate! {
    to self.page {
      pub fn get_header(&self) -> &PageHeader;
    }
  }
}

#[cfg(test)]
mod test {
  use crate::common::buffer::PageBuffer;
  use crate::common::ids::PageId;
  use crate::common::page::{PageFlag, PageHeader};
  use crate::pages::meta::{Meta, MetaPage};
  use aligners::{alignment, AlignedBytes};

  #[test]
  pub fn test_meta_page() {
    let mut bytes: AlignedBytes<alignment::Page> = AlignedBytes::new_zeroed(4096);

    let meta0 = PageHeader::init_meta(PageId::of(0));

    let mut meta = Meta::default();
    meta.page_size = 3;
    meta.version = 2;
    MetaPage::write(&mut bytes, &meta0, &meta);

    let page = PageBuffer::owned_bytes(bytes);
    let metapage = MetaPage::new(page);

    let header = metapage.get_header();
    assert_eq!(header.flags(), PageFlag::META);
    let meta = metapage.get_meta();
    let fnv = meta.sum64();
    assert_eq!(meta.version, 2);
  }
}
