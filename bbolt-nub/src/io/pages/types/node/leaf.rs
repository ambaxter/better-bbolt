use crate::common::layout::node::{BranchElement, LeafElement};
use crate::common::layout::page::PageHeader;
use crate::io::pages::types::node::branch::BranchPage;
use crate::io::pages::types::node::{HasKeys, HasValues};
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use bytemuck::{cast_slice, from_bytes};
use delegate::delegate;
use std::ops::RangeBounds;

pub struct LeafPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
}

impl<'tx, T: 'tx> Page for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T: 'tx> GetKvRefSlice for LeafPage<'tx, T>
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

impl<'tx, T: 'tx> LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn elements(&self) -> &[LeafElement] {
    let elements_len = self.page.page_header().count() as usize;
    let elements_start = size_of::<PageHeader>();
    let elements_end = elements_start + (elements_len * size_of::<LeafElement>());
    cast_slice(&self.page.root_page()[elements_start..elements_end])
  }
}

impl<'tx, T: 'tx> HasKeys<'tx> for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type RefKv<'a>
    = T::RefKv<'a>
  where
    Self: 'a;
  type TxKv = T::TxKv;

  fn key_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<LeafElement>());
    let element_end = element_start + size_of::<LeafElement>();
    let element: &LeafElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    let kv_start = element_start + element.key_dist() as usize;
    let key_end = kv_start + element.key_len() as usize;
    Some(self.page.get_ref_slice(kv_start..key_end))
  }

  fn key(&self, index: usize) -> Option<Self::TxKv> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<LeafElement>());
    let element_end = element_start + size_of::<LeafElement>();
    let element: &LeafElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    let kv_start = element_start + element.key_dist() as usize;
    let key_end = kv_start + element.key_len() as usize;
    Some(self.page.get_tx_slice(kv_start..key_end))
  }
}

impl<'tx, T: 'tx> HasValues<'tx> for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn value_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<BranchElement>());
    let element_end = element_start + size_of::<BranchElement>();
    let element: &BranchElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    let kv_start = element_start + element.key_dist() as usize;
    let value_start = kv_start + element.key_len() as usize;
    let value_end = value_start + element.key_len() as usize;
    Some(self.page.get_ref_slice(value_start..value_end))
  }

  fn value(&self, index: usize) -> Option<Self::TxKv> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<BranchElement>());
    let element_end = element_start + size_of::<BranchElement>();
    let element: &BranchElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    let kv_start = element_start + element.key_dist() as usize;
    let value_start = kv_start + element.key_len() as usize;
    let value_end = value_start + element.key_len() as usize;
    Some(self.page.get_tx_slice(value_start..value_end))
  }
}
