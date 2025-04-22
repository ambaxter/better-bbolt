use crate::common::id::NodePageId;
use crate::common::layout::node::BranchElement;
use crate::io::pages::types::node::branch::{HasBranches, HasNodes, HasSearchBranch};
use crate::io::pages::types::node::{HasElements, HasKeyRefs, HasKeys};
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use delegate::delegate;
use std::ops::RangeBounds;

#[derive(Clone)]
pub struct BBoltBranch<'tx, T> {
  page: TxPage<'tx, T>,
}

impl<'tx, T> BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub fn new(page: TxPage<T>) -> BBoltBranch<T> {
    BBoltBranch { page }
  }
}

impl<'tx, T> Page for BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'a, 'tx, T> GatKvRef<'a> for BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  type KvRef = <T as GatKvRef<'a>>::KvRef;
}

impl<'tx, T> GetGatKvRefSlice for BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    self.page.get_ref_slice(range)
  }
}

impl<'tx, T> BBoltBranch<'tx, T> where T: TxPageType<'tx> {}

impl<'tx, T> HasElements<'tx> for BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Element = BranchElement;
}

impl<'tx, T> HasSearchBranch<'tx> for BBoltBranch<'tx, T> where T: TxPageType<'tx> {}

impl<'tx, T> HasKeyRefs for BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn key_ref<'a>(&'a self, index: usize) -> Option<<Self as GatKvRef<'a>>::KvRef> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_ref_slice(key_range))
  }
}

impl<'tx, T> HasKeys<'tx> for BBoltBranch<'tx, T>
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

impl<'tx, T> HasNodes<'tx> for BBoltBranch<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn node(&self, index: usize) -> Option<NodePageId> {
    self.elements().get(index).map(|element| element.page_id())
  }
}

impl<'tx, T> HasBranches<'tx> for BBoltBranch<'tx, T> where T: TxPageType<'tx> {}
