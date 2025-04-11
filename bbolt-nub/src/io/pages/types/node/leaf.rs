use crate::common::layout::node::LeafElement;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::{HasElements, HasKeyPosLen, HasKeys, HasValues};
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use delegate::delegate;
use std::ops::{Range, RangeBounds};

#[derive(Clone)]
pub struct LeafPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
}

impl<'tx, T: 'tx> LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub fn new(page: TxPage<'tx, T>) -> Self {
    Self { page }
  }
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
  pub fn search_leaf<'a>(&'a self, v: &'a [u8]) -> Option<usize>
  where
    <Self as GetKvRefSlice>::RefKv<'a>: PartialOrd<[u8]>,
  {
    self.search(v).ok()
  }

  pub fn try_search_leaf<'a>(
    &'a self, v: &'a [u8],
  ) -> crate::Result<Option<usize>, <<Self as GetKvRefSlice>::RefKv<'a> as TryPartialEq<[u8]>>::Error>
  where
    <Self as GetKvRefSlice>::RefKv<'a>: TryPartialOrd<[u8]>,
  {
    self.try_search(v).map(|r| r.ok())
  }

  fn value_range(&self, index: usize) -> Option<Range<usize>> {
    self.elements().get(index).map(|element| {
      let start = element.kv_data_start(index) + element.elem_key_len();
      let end = start + element.value_len() as usize;
      start..end
    })
  }
}

impl<'tx, T: 'tx> HasElements<'tx> for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Element = LeafElement;
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

impl<'tx, T: 'tx> HasValues<'tx> for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn value_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>> {
    self
      .value_range(index)
      .map(|value_range| self.page.get_ref_slice(value_range))
  }

  fn key_value_ref<'a>(&'a self, index: usize) -> Option<(Self::RefKv<'a>, Self::RefKv<'a>)> {
    let key_range = self.key_range(index)?;
    let value_range = self.value_range(index)?;
    Some((
      self.page.get_ref_slice(key_range),
      self.page.get_ref_slice(value_range),
    ))
  }

  fn value(&self, index: usize) -> Option<Self::TxKv> {
    self
      .value_range(index)
      .map(|value_range| self.page.get_tx_slice(value_range))
  }

  fn key_value(&self, index: usize) -> Option<(Self::TxKv, Self::TxKv)> {
    let key_range = self.key_range(index)?;
    let value_range = self.value_range(index)?;
    Some((
      self.page.get_tx_slice(key_range),
      self.page.get_tx_slice(value_range),
    ))
  }
}
