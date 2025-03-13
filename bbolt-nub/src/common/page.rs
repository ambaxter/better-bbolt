use crate::common::id::{DbPageId, DbPageType, FreelistPageId, MetaPageId, NodePageId};
use std::borrow::Cow;
use std::fmt::Debug;
use error_stack::{Result, Report};
use crate::common::errors::PageError;

pub trait FastCheckPageFlag: DbPageType {
  fn page_flag_mask() -> PageFlag;
}

impl FastCheckPageFlag for MetaPageId {
  fn page_flag_mask() -> PageFlag {
    PageFlag::META
  }
}

impl FastCheckPageFlag for FreelistPageId {
  fn page_flag_mask() -> PageFlag {
    PageFlag::FREELIST
  }
}

impl FastCheckPageFlag for NodePageId {
  fn page_flag_mask() -> PageFlag {
    PageFlag::NODE_TYPE_MASK
  }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PageFlag(u16);

bitflags::bitflags! {
  impl PageFlag: u16 {
    const BRANCH = 0x01;
    const LEAF = 0x02;
    const META = 0x04;
    const FREELIST = 0x10;
    const NODE_TYPE_MASK = 0x01 | 0x02;
    const PAGE_TYPE_MASK = 0x01 | 0x02 | 0x04 | 0x10;
  }
}

/// `PageHeader` represents the on-file layout of a page header.
///
/// `page` in Go BBolt
#[repr(C)]
#[derive(
  Debug,
  Copy,
  Clone,
  Default,
  getset::CopyGetters,
  getset::Setters,
  bytemuck::Pod,
  bytemuck::Zeroable,
)]
#[getset(get_copy = "pub", set = "pub")]
pub struct PageHeader {
  /// This Page's ID
  id: DbPageId,
  /// Page's type. Branch(0x01), Leaf(0x02), Meta(0x04), or FreeList(0x10)
  flags: PageFlag,
  /// Defines the number of items in the Branch, Leaf, and Freelist pages
  count: u16,
  #[getset(skip)]
  /// How many additional meta.page_size pages are included in this page
  overflow: u32,
}

impl PageHeader {
  #[inline(always)]
  pub fn get_overflow(&self) -> u32 {
    self.overflow
  }

  #[inline(always)]
  pub unsafe fn set_overflow(&mut self, overflow: u32) -> &mut Self {
    self.overflow = overflow;
    self
  }

  #[inline(always)]
  pub fn is_node(&self) -> bool {
    (self.flags & PageFlag::NODE_TYPE_MASK) != PageFlag::empty()
  }

  #[inline(always)]
  pub fn is_branch(&self) -> bool {
    (self.flags & PageFlag::PAGE_TYPE_MASK) == PageFlag::BRANCH
  }

  #[inline(always)]
  pub fn is_leaf(&self) -> bool {
    (self.flags & PageFlag::PAGE_TYPE_MASK) == PageFlag::LEAF
  }

  #[inline(always)]

  pub fn is_meta(&self) -> bool {
    (self.flags & PageFlag::PAGE_TYPE_MASK) == PageFlag::META
  }

  #[inline(always)]
  pub fn is_freelist(&self) -> bool {
    (self.flags & PageFlag::PAGE_TYPE_MASK) == PageFlag::FREELIST
  }

  #[inline(always)]
  pub fn init_meta(id: DbPageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::META,
      count: 0,
      overflow: 0,
    }
  }

  #[inline(always)]
  pub fn init_freelist(id: DbPageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::FREELIST,
      count: 0,
      overflow: 0,
    }
  }

  #[inline(always)]
  pub fn init_leaf(id: DbPageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::LEAF,
      count: 0,
      overflow: 0,
    }
  }

  #[inline(always)]
  pub fn init_branch(id: DbPageId) -> Self {
    PageHeader {
      id,
      flags: PageFlag::BRANCH,
      count: 0,
      overflow: 0,
    }
  }

  pub fn fast_check<P: FastCheckPageFlag>(&self, id: P) -> Result<(), PageError> {
    if *id != self.id {
      Err(Report::new(PageError::UnexpectedDbPageId(self.id, *id)))
    } else if P::page_flag_mask() & self.flags == PageFlag::empty() {
      Err(Report::new(PageError::InvalidPageFlag(P::page_flag_mask(), self.flags)))
    } else {
      Ok(())
    }
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
}

#[cfg(test)]
mod tests {
  use crate::common::page::PageHeader;

  #[test]
  fn test() {
    let mut p = PageHeader::default();
  }
}
