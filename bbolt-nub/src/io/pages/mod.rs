use crate::common::errors::DiskReadError;
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::TxSlot;
use crate::io::backends::IOPageReader;
use crate::io::bytes::TxBytes;
use bytemuck::from_bytes;
use delegate::delegate;
use std::cmp;
use std::collections::Bound;
use std::ops::{Deref, Range, RangeBounds};

pub mod kv;

pub mod io;

pub mod loaded;

pub mod lazy;

pub mod types;

pub trait RefIntoCopiedIter {
  type Iter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a;
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a>;
}

pub trait KvDataType: Ord + RefIntoCopiedIter {
  fn cmp(&self, other: &[u8]) -> cmp::Ordering;

  fn eq(&self, other: &[u8]) -> bool;

  fn lt(&self, other: &[u8]) -> bool;
  fn le(&self, other: &[u8]) -> bool;

  fn gt(&self, other: &[u8]) -> bool;
  fn ge(&self, other: &[u8]) -> bool;
}

pub trait SubRange {
  fn sub_range<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

impl SubRange for Range<usize> {
  fn sub_range<R: RangeBounds<usize>>(&self, range: R) -> Self {
    let start = match range.start_bound().cloned() {
      Bound::Included(start) => self.start + start,
      Bound::Excluded(start) => self.start + start + 1,
      Bound::Unbounded => self.start,
    };
    let end = match range.end_bound().cloned() {
      Bound::Included(end) => self.start + end + 1,
      Bound::Excluded(end) => self.start + end,
      Bound::Unbounded => self.end,
    };
    assert!(
      start <= end,
      "New start ({start}) should be <= new end ({end})"
    );
    assert!(
      end <= self.end,
      "New end ({end}) should be <= current end ({0})",
      self.end
    );
    start..end
  }
}

pub trait GetKvRefSlice {
  type RefKv<'a>: GetKvRefSlice + KvDataType + 'a
  where
    Self: 'a;
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a>;
}

pub trait GetKvTxSlice<'tx>: GetKvRefSlice {
  type TxKv: GetKvTxSlice<'tx> + KvDataType + 'tx;
  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv;
}

pub trait Page {
  #[inline]
  fn page_header(&self) -> &PageHeader {
    from_bytes(&self.root_page()[0..size_of::<PageHeader>()])
  }

  fn root_page(&self) -> &[u8];
}

pub trait TxPageType<'tx>: Page + GetKvTxSlice<'tx> + GetKvRefSlice + Sync + Send {
  type TxPageBytes: TxBytes<'tx>;
}

pub struct TxPage<'tx, T: 'tx> {
  tx: TxSlot<'tx>,
  page: T,
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
  ) -> crate::Result<Self::TxPageType, DiskReadError>;

  fn read_freelist_page(
    &'tx self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError>;

  fn read_node_page(
    &'tx self, node_page_id: NodePageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError>;
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
