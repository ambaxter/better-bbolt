use crate::common::errors::{DiskReadError, PageError};
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::TxSlot;
use crate::io::backends::IOPageReader;
use crate::io::bytes::TxBytes;
use crate::io::ops::{
  GetKvRefSlice, GetKvTxSlice, RefIntoCopiedIter, RefIntoTryBuf, TryGet, TryPartialEq,
  TryPartialOrd,
};
use crate::io::pages::types::freelist::FreelistPage;
use crate::io::pages::types::meta::MetaPage;
use crate::io::pages::types::node::NodePage;
use bytemuck::from_bytes;
use delegate::delegate;
use std::hash::Hash;
use std::ops::{Deref, RangeBounds};

pub mod io;

pub mod loaded;

//pub mod lazy;

pub mod types;

pub trait Page {
  #[inline]
  fn page_header(&self) -> &PageHeader {
    from_bytes(&self.root_page()[0..size_of::<PageHeader>()])
  }

  fn root_page(&self) -> &[u8];
}

pub trait TxPageType<'tx>: Page + GetKvTxSlice<'tx> + GetKvRefSlice + Clone + Sync + Send {
  type TxPageBytes: TxBytes<'tx>;
}

#[derive(Clone)]
pub struct TxPage<'tx, T: 'tx> {
  tx: TxSlot<'tx>,
  page: T,
}

impl<'tx, T: 'tx> TxPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub fn new(page: T) -> Self {
    TxPage {
      tx: Default::default(),
      page,
    }
  }
}

impl<'tx, T: 'tx> Page for TxPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T: 'tx> GetKvRefSlice for TxPage<'tx, T>
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

impl<'tx, T: 'tx> GetKvTxSlice<'tx> for TxPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type TxKv = T::TxKv;

  #[inline]
  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    self.page.get_tx_slice(range)
  }
}

pub trait TxReadPageIO<'tx> {
  type TxPageType: TxPageType<'tx>;

  fn read_meta_page(
    &'tx self, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<'tx, Self::TxPageType>, PageError>;

  fn read_freelist_page(
    &'tx self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<'tx, Self::TxPageType>, PageError>;

  fn read_node_page(
    &'tx self, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<'tx, Self::TxPageType>, PageError>;
}

pub trait TxReadLoadedPageIO<'tx>: TxReadPageIO<'tx> {}

pub trait TxReadLazyPageIO<'tx>: TxReadPageIO<'tx> {
  fn read_freelist_overflow(
    &'tx self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError>;

  fn read_node_overflow(
    &'tx self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError>;
}
