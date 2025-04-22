use crate::common::layout::node::{LeafElement, LeafFlag};
use crate::io::pages::GatKvRef;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::{HasElements, HasKeyPosLen, HasKeys};

pub mod bbolt;

impl HasKeyPosLen for LeafElement {
  #[inline]
  fn elem_key_dist(&self) -> usize {
    self.key_dist() as usize
  }

  #[inline]
  fn elem_key_len(&self) -> usize {
    self.key_len() as usize
  }
}

pub trait HasSearchLeaf<'tx>: HasElements<'tx> {
  fn search_leaf<'a>(&'a self, v: &[u8]) -> Result<usize, usize>
  where
    <Self as GatKvRef<'a>>::KvRef: PartialOrd<[u8]>,
  {
    self
      .search(v)
      .map_err(|next_index| next_index.saturating_sub(1))
  }

  fn try_search_leaf<'a>(
    &'a self, v: &[u8],
  ) -> crate::Result<
    Result<usize, usize>,
    <<Self as GatKvRef<'a>>::KvRef as TryPartialEq<[u8]>>::Error,
  >
  where
    <Self as GatKvRef<'a>>::KvRef: TryPartialOrd<[u8]>,
  {
    self
      .try_search(v)
      .map(|r| r.map_err(|next_index| next_index.saturating_sub(1)))
  }
}

pub trait HasValues<'tx>: HasKeys<'tx> {
  fn leaf_flag(&self, index: usize) -> Option<LeafFlag>;

  fn value_ref<'a>(&'a self, index: usize) -> Option<<Self as GatKvRef<'a>>::KvRef>;

  fn key_value_ref<'a>(
    &'a self, index: usize,
  ) -> Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)>;

  fn value(&self, index: usize) -> Option<Self::TxKv>;

  fn key_value(&self, index: usize) -> Option<(Self::TxKv, Self::TxKv)>;
}

pub trait HasLeaves<'tx>: HasValues<'tx> + HasSearchLeaf<'tx> + Clone {}
