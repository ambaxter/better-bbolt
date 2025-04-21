use crate::common::layout::node::{LeafElement, LeafFlag};
use crate::io::bytes::shared_bytes::SharedRefSlice;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::{HasElements, HasKeyPosLen, HasKeyRefs, HasKeys, HasValues};
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
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

impl<'a, 'tx, T: 'tx> GatKvRef<'a> for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type KvRef = <T as GatKvRef<'a>>::KvRef;
}

impl<'tx, T: 'tx> GetGatKvRefSlice for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    self.page.get_ref_slice(range)
  }
}

impl<'tx, T: 'tx> LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub(crate) fn search_leaf<'a>(&'a self, v: &[u8]) -> Result<usize, usize>
  where
    <Self as GatKvRef<'a>>::KvRef: PartialOrd<[u8]>,
  {
    self
      .search(v)
      .map_err(|next_index| next_index.saturating_sub(1))
  }

  pub(crate) fn try_search_leaf<'a>(
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

impl<'tx, T: 'tx> HasKeyRefs for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn key_ref<'a>(&'a self, index: usize) -> Option<<Self as GatKvRef<'a>>::KvRef> {
    self
      .key_range(index)
      .map(|key_range| self.page.get_ref_slice(key_range))
  }
}

impl<'tx, T: 'tx> HasKeys<'tx> for LeafPage<'tx, T>
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

impl<'tx, T: 'tx> HasValues<'tx> for LeafPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn leaf_flag(&self, index: usize) -> Option<LeafFlag> {
    self.elements().get(index).map(|element| element.flags())
  }

  fn value_ref<'a>(&'a self, index: usize) -> Option<<Self as GatKvRef<'a>>::KvRef> {
    self
      .value_range(index)
      .map(|value_range| self.page.get_ref_slice(value_range))
  }

  fn key_value_ref<'a>(
    &'a self, index: usize,
  ) -> Option<(<Self as GatKvRef<'a>>::KvRef, <Self as GatKvRef<'a>>::KvRef)> {
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
