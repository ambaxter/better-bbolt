use bbolt_index::cursor::Cursor;
use bbolt_index::index::BucketIndex;
use bbolt_index::pages::node::NodePage;
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Deref;

#[derive(Clone)]
pub enum BucketKey<'tx> {
  Page(NodePage<'tx>, usize),
  Mapped(&'tx [u8]),
}

impl<'tx> Deref for BucketKey<'tx> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      BucketKey::Page(page, index) => page.key(*index).expect("BucketKey.borrow"),
      BucketKey::Mapped(map) => *map,
    }
  }
}

impl<'tx> PartialEq for BucketKey<'tx> {
  fn eq(&self, other: &Self) -> bool {
    self.deref() == other.deref()
  }
}

impl<'tx> Eq for BucketKey<'tx> {}

impl<'tx> PartialOrd for BucketKey<'tx> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.deref().partial_cmp(other.deref())
  }
}

impl<'tx> Ord for BucketKey<'tx> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.deref().cmp(&other.deref())
  }
}

#[derive(Copy, Clone)]
pub enum BucketDelta<'tx> {
  Update(&'tx [u8]),
  Delete,
}

pub struct Bucket<'tx> {
  index: BucketIndex<'tx>,
}

pub struct BucketMut<'tx> {
  bucket_delta: BTreeMap<BucketKey<'tx>, BucketDelta<'tx>>,
}
