use crate::common::id::NodePageId;
use crate::common::layout::node::{BranchElement, LeafElement};
use crate::common::layout::page::PageHeader;
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::types::node::{HasElements, HasKeyPosLen, HasKeys, HasNodes};
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use bytemuck::{cast_slice, from_bytes};
use delegate::delegate;
use std::ops::{Range, RangeBounds};

#[derive(Clone)]
pub struct BranchPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
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

impl<'tx, T: 'tx> GetKvRefSlice for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type RefKv<'a>
    = T::RefKv<'a>
  where
    Self: 'a;

  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    self.page.get_ref_slice(range)
  }
}

impl<'tx, T: 'tx> BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn search_branch(&self, v: &[u8]) -> usize {
    self
      .search(v)
      .unwrap_or_else(|next_index| next_index.saturating_sub(1))
  }
}

impl<'tx, T: 'tx> HasElements<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Element = BranchElement;
}

impl<'tx, T: 'tx> HasKeys<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type RefKv<'a>
    = T::RefKv<'a>
  where
    Self: 'a;
  type TxKv = T::TxKv;

  fn key_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_ref_slice(key_range))
  }

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
