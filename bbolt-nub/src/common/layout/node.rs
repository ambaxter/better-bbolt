use crate::common::id::NodePageId;
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Setters};

///`BranchElement` represents the on-file layout of a branch page's element
///
#[repr(C)]
#[derive(Debug, Copy, Clone, CopyGetters, Setters, Pod, Zeroable)]
pub struct BranchElement {
  #[getset(set = "pub")]
  /// The distance from this element's pointer to its key location
  key_dist: u32,
  #[getset(set = "pub")]
  /// Key length
  key_len: u32,
  /// Page ID of this branch
  page_id: NodePageId,
}

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
