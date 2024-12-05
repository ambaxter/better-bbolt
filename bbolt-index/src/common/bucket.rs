use crate::common::buffer::{OwnedBuffer, PageBuffer};
use crate::common::ids::{BucketPageId, NodePageId};
use crate::common::page::PageHeader;
use crate::pages::node::{LeafValue, NodePage, NodeType};
use bytemuck::{Pod, Zeroable};
use getset::{CopyGetters, Getters, MutGetters, Setters};

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
    format!("<pgid={},seq={}>", value.root, value.sequence)
  }
}

pub enum BucketBuffer<'tx> {
  InlinePage(NodePage<'tx>, usize),
  Owned(NodePage<'tx>),
}

impl<'tx> BucketBuffer<'tx> {
  pub fn get_header(&self) -> &PageHeader {
    match self {
      BucketBuffer::InlinePage(parent, index) => match parent.access() {
        NodeType::Branch(_) => panic!("{:?} is not a leaf page", parent.get_header().id()),
        NodeType::Leaf(leaf) => match leaf.get_value(*index).unwrap() {
          LeafValue::Bucket(bucket) => bytemuck::from_bytes(&bucket[0..size_of::<PageHeader>()]),
          LeafValue::Value(_) => panic!(
            "{:?} is not a bucket in page {:?}",
            index,
            parent.get_header().id()
          ),
        },
      },
      BucketBuffer::Owned(o) => o.get_header(),
    }
  }
}
