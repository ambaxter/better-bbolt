use crate::common::errors::DiskReadError;
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId};
use crate::common::page::PageHeader;
use crate::tx_io::backends::ReadIO;
use crate::tx_io::bytes::TxBytes;
use crate::tx_io::pages::complete::CompletePage;
use std::ops::{Deref, RangeBounds};

pub mod complete;

pub mod lazy;

pub trait RefIntoCopiedIter {
  type Iter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a;
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a>;
}

pub trait KvDataType: Ord + RefIntoCopiedIter {
  fn partial_eq(&self, other: &[u8]) -> bool;

  fn lt(&self, other: &[u8]) -> bool;
  fn le(&self, other: &[u8]) -> bool;

  fn gt(&self, other: &[u8]) -> bool;
  fn ge(&self, other: &[u8]) -> bool;
}

pub trait GetKvSlice<'tx> {
  type RefKv<'a>: KvDataType + 'a
  where
    Self: 'a;
  type TxKv: KvDataType + 'tx;
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a>;
  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv;
}

pub trait Page<'tx> {
  fn header(&self) -> &PageHeader;
  fn root_page(&self) -> &[u8];
}

pub trait ReadPageIO<'tx> {
  type PageBytes: TxBytes<'tx>;

  fn read_meta_page(&self, page_id: MetaPageId) -> crate::Result<Self::PageBytes, DiskReadError>;

  fn read_freelist_page(
    &self, page_id: FreelistPageId,
  ) -> crate::Result<Self::PageBytes, DiskReadError>;

  fn read_node_page(&self, page_id: NodePageId) -> crate::Result<Self::PageBytes, DiskReadError>;
}

pub trait ReadCompletePageIO<'tx>: ReadPageIO<'tx> {}

pub trait ReadLazyPageIO<'tx>: ReadPageIO<'tx> {
  fn read_freelist_overflow(
    &self, page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::PageBytes, DiskReadError>;

  fn read_node_overflow(
    &self, page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::PageBytes, DiskReadError>;
}
