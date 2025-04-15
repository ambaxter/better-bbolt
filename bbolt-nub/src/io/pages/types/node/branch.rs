use crate::common::id::NodePageId;
use crate::common::layout::node::BranchElement;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::types::node::{HasElements, HasKeyRefs, HasKeys, HasNodes};
use crate::io::pages::{GatRefKv, GetGatKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use delegate::delegate;
use std::ops::RangeBounds;

#[derive(Clone)]
pub struct BranchPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
}

impl<'tx, T: 'tx> BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub fn new(page: TxPage<T>) -> BranchPage<T> {
    BranchPage { page }
  }
}

impl<'tx, T: 'tx> Page for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'a, 'tx, T: 'tx> GatRefKv<'a> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type RefKv = <T as GatRefKv<'a>>::RefKv;
}

impl<'tx, T: 'tx> GetGatKvRefSlice for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatRefKv<'a>>::RefKv {
    self.page.get_ref_slice(range)
  }
}

impl<'tx, T: 'tx> BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub(crate) fn search_branch<'a>(&'a self, v: &[u8]) -> usize
  where
    <Self as GatRefKv<'a>>::RefKv: PartialOrd<[u8]>,
  {
    self
      .search(v)
      .unwrap_or_else(|next_index| next_index.saturating_sub(1))
  }

  pub(crate) fn try_search_branch<'a>(
    &'a self, v: &[u8],
  ) -> crate::Result<usize, <<Self as GatRefKv<'a>>::RefKv as TryPartialEq<[u8]>>::Error>
  where
    <Self as GatRefKv<'a>>::RefKv: TryPartialOrd<[u8]>,
  {
    self
      .try_search(v)
      .map(|r| r.unwrap_or_else(|next_index| next_index.saturating_sub(1)))
  }
}

impl<'tx, T: 'tx> HasElements<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Element = BranchElement;
}

impl<'tx, T: 'tx> HasKeyRefs for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type RefKv = T::RefKv;

  fn key_ref<'a>(&'a self, index: usize) -> Option<<Self::RefKv as GatRefKv<'a>>::RefKv> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_ref_slice(key_range))
  }
}

impl<'tx, T: 'tx> HasKeys<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type TxKv = T::TxKv;

  fn key(&self, index: usize) -> Option<Self::TxKv> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_tx_slice(key_range))
  }
}

impl<'tx, T: 'tx> HasNodes<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn node(&self, index: usize) -> Option<NodePageId> {
    self.elements().get(index).map(|element| element.page_id())
  }
}
