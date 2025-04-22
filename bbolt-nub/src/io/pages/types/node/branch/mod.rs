use crate::common::id::NodePageId;
use crate::common::layout::node::BranchElement;
use crate::io::pages::GatKvRef;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::{HasElements, HasKeyPosLen, HasKeys};

pub mod bbolt;

pub trait HasSearchBranch<'tx>: HasElements<'tx> {
  fn search_branch<'a>(&'a self, v: &[u8]) -> usize
  where
    <Self as GatKvRef<'a>>::KvRef: PartialOrd<[u8]>,
  {
    self
      .search(v)
      .unwrap_or_else(|next_index| next_index.saturating_sub(1))
  }

  fn try_search_branch<'a>(
    &'a self, v: &[u8],
  ) -> crate::Result<usize, <<Self as GatKvRef<'a>>::KvRef as TryPartialEq<[u8]>>::Error>
  where
    <Self as GatKvRef<'a>>::KvRef: TryPartialOrd<[u8]>,
  {
    self
      .try_search(v)
      .map(|r| r.unwrap_or_else(|next_index| next_index.saturating_sub(1)))
  }
}

pub trait HasBranches<'tx>: HasNodes<'tx> + HasSearchBranch<'tx> + Clone {}

pub trait HasNodes<'tx>: HasKeys<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

impl HasKeyPosLen for BranchElement {
  #[inline]
  fn elem_key_dist(&self) -> usize {
    self.key_dist() as usize
  }

  #[inline]
  fn elem_key_len(&self) -> usize {
    self.key_len() as usize
  }
}
