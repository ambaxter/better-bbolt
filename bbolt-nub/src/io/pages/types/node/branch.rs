use crate::common::id::NodePageId;
use crate::common::layout::node::BranchElement;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::types::node::{
  HasBranches, HasElements, HasKeyRefs, HasKeys, HasNodes, HasSearchBranch, HasSearchLeaf,
};
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use delegate::delegate;
use std::ops::RangeBounds;

#[derive(Clone)]
pub struct BranchPage<'tx, T> {
  page: TxPage<'tx, T>,
}

impl<'tx, T> BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub fn new(page: TxPage<T>) -> BranchPage<T> {
    BranchPage { page }
  }
}

impl<'tx, T> Page for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'a, 'tx, T> GatKvRef<'a> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type KvRef = <T as GatKvRef<'a>>::KvRef;
}

impl<'tx, T> GetGatKvRefSlice for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    self.page.get_ref_slice(range)
  }
}

impl<'tx, T> BranchPage<'tx, T> where T: TxPageType<'tx> {}

impl<'tx, T> HasElements<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Element = BranchElement;
}

impl<'tx, T> HasSearchBranch<'tx> for BranchPage<'tx, T> where T: TxPageType<'tx> {}

impl<'tx, T> HasKeyRefs for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn key_ref<'a>(&'a self, index: usize) -> Option<<Self as GatKvRef<'a>>::KvRef> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_ref_slice(key_range))
  }
}

impl<'tx, T> HasKeys<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type TxKv = T::KvTx;

  fn key(&self, index: usize) -> Option<Self::TxKv> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_tx_slice(key_range))
  }
}

impl<'tx, T> HasNodes<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn node(&self, index: usize) -> Option<NodePageId> {
    self.elements().get(index).map(|element| element.page_id())
  }
}

impl<'tx, T> HasBranches<'tx> for BranchPage<'tx, T> where T: TxPageType<'tx> {}
