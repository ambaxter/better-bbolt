use bytemuck::{Pod, Zeroable};
use getset::CopyGetters;
use crate::common::id::BucketPageId;

/// `BucketHeader` represents the on-file layout of a bucket header.
/// This is stored as the "value" of a bucket key. If the bucket is small enough,
/// then its root page can be stored inline in the "value", after the bucket
/// header. In the case of inline buckets, the "root" will be 0.
///
/// `bucket` in Go BBolt
#[repr(C)]
#[derive(Debug, Default, Copy, Clone, CopyGetters, Zeroable, Pod, Eq, PartialEq)]
#[getset(get_copy = "pub")]
pub struct BucketHeader {
  /// page id of the bucket's root-level page
  root: BucketPageId,
  /// monotonically incrementing, used by NextSequence()
  sequence: u64,
}

impl BucketHeader {
  pub fn new(root: BucketPageId, sequence: u64) -> BucketHeader {
    BucketHeader { root, sequence }
  }

  pub fn inc_sequence(&mut self) {
    self.sequence += 1;
  }
}

impl From<BucketHeader> for String {
  fn from(value: BucketHeader) -> Self {
    format!("<pgid={:?},seq={}>", value.root, value.sequence)
  }
}