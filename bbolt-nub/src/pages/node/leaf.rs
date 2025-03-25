use crate::common::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, TxPage};
use crate::pages::Page;
use crate::pages::node::branch::{BranchElement, BranchPage};
use bytemuck::{Pod, Zeroable};
use delegate::delegate;
use getset::{CopyGetters, Setters};

#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct LeafFlag(u32);

bitflags::bitflags! {
  impl LeafFlag: u32 {
    const BUCKET = 0x01;
  }
}

/// `LeafElement` represents the on-file layout of a leaf page's element
///
#[repr(C)]
#[derive(Debug, Copy, Clone, CopyGetters, Setters, Pod, Zeroable)]
pub struct LeafElement {
  #[getset(get_copy = "pub")]
  /// Additional flag for each element. If leaf is a Bucket then 0x01 set
  flags: LeafFlag,
  #[getset(set = "pub")]
  /// The distance from this element's pointer to its key/value location
  key_dist: u32,
  #[getset(set = "pub")]
  /// Key length
  key_len: u32,
  #[getset(set = "pub")]
  /// Value length
  value_len: u32,
}

#[derive(Clone)]
pub struct LeafPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for LeafPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T> LeafPage<T>
where
  T: TxPage<'tx>,
{
  fn elements(&self) -> &[LeafElement] {
    let count = self.page_header().count() as usize;
    let elem_start = size_of::<PageHeader>();
    let elem_end = elem_start + (size_of::<LeafElement>() * count);
    bytemuck::cast_slice(&self.root_page()[elem_start..elem_end])
  }
}
