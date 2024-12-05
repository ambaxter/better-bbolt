use crate::common::ids::PageId;
use getset::{CopyGetters, Setters};
use std::borrow::Cow;
use std::cmp::Ordering;

#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PageFlag(u16);

bitflags::bitflags! {
  impl PageFlag: u16 {
    const BRANCH = 0x01;
    const LEAF = 0x02;
    const META = 0x04;
    const FREELIST = 0x10;
    const PAGE_TYPE_MASK = 0x01 | 0x02 | 0x04 | 0x10;
  }
}

/// `PageHeader` represents the on-file layout of a page header.
///
/// `page` in Go BBolt
#[repr(C)]
#[derive(Debug, Copy, Clone, Default, CopyGetters, Setters, bytemuck::Pod, bytemuck::Zeroable)]
#[getset(get_copy = "pub", set = "pub")]
pub struct PageHeader {
  /// This Page's ID
  id: PageId,
  /// Page's type. Branch(0x01), Leaf(0x02), Meta(0x04), or FreeList(0x10)
  flags: PageFlag,
  /// Defines the number of items in the Branch, Leaf, and Freelist pages
  count: u16,
  #[getset(skip)]
  /// How many additional meta.page_size pages are included in this page
  overflow: u32,
}

impl PageHeader {}

impl PartialOrd for PageHeader {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for PageHeader {
  fn cmp(&self, other: &Self) -> Ordering {
    self.id.cmp(&other.id)
  }
}

impl PartialEq for PageHeader {
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
  }
}

impl Eq for PageHeader {}

impl PageHeader {
  pub fn init_meta(id: PageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::META,
      count: 0,
      overflow: 0,
    }
  }

  pub fn init_freelist(id: PageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::FREELIST,
      count: 0,
      overflow: 0,
    }
  }

  pub fn init_leaf(id: PageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::LEAF,
      count: 0,
      overflow: 0,
    }
  }

  pub fn init_branch(id: PageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::BRANCH,
      count: 0,
      overflow: 0,
    }
  }

  #[inline]
  pub fn get_page_id<T: From<PageId>>(&self) -> T {
    self.id.into()
  }

  #[inline]
  pub fn is_branch(&self) -> bool {
    self.flags & PageFlag::PAGE_TYPE_MASK == PageFlag::BRANCH
  }

  #[inline]
  pub fn is_leaf(&self) -> bool {
    self.flags & PageFlag::PAGE_TYPE_MASK == PageFlag::LEAF
  }

  #[inline]
  pub fn is_meta(&self) -> bool {
    self.flags & PageFlag::PAGE_TYPE_MASK == PageFlag::META
  }

  #[inline]
  pub fn is_freelist(&self) -> bool {
    self.flags & PageFlag::PAGE_TYPE_MASK == PageFlag::FREELIST
  }

  pub fn overflow(&self) -> u32 {
    self.overflow
  }

  #[inline]
  pub unsafe fn set_overflow(&mut self, overflow: u32) {
    self.overflow = overflow;
  }

  pub fn fast_check(&self, id: PageId) {
    assert_eq!(
      self.id, id,
      "Page expected to be {}, but self identifies as {}",
      id, self.id
    );
    assert_eq!(
      (self.flags & PageFlag::PAGE_TYPE_MASK).bits().count_ones(),
      1,
      "page {}: has unexpected type/flags {:#x}",
      self.id,
      self.flags.bits()
    );
  }

  /// page_type returns a human-readable page type string used for debugging.
  pub fn page_type(&self) -> Cow<'static, str> {
    match self.flags & PageFlag::PAGE_TYPE_MASK {
      PageFlag::BRANCH => Cow::Borrowed("branch"),
      PageFlag::LEAF => Cow::Borrowed("leaf"),
      PageFlag::META => Cow::Borrowed("meta"),
      PageFlag::FREELIST => Cow::Borrowed("freelist"),
      _ => Cow::Owned(format!("unknown<{:#x}>", self.flags.bits())),
    }
  }

  pub fn write(data: &mut [u8], page_header: &PageHeader) {
    let header_bytes = bytemuck::bytes_of(page_header);
    let header_len = header_bytes.len();
    data[0..header_len].copy_from_slice(header_bytes);
  }
}

#[cfg(test)]
mod test {}
