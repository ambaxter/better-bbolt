use crate::common::errors::PageError;
use crate::common::id::DbPageType;
use crate::common::layout::page::PageHeader;
use crate::io::NonContigReader;
use crate::io::pages::lazy_page::{LazySlice, LazySliceIter};
use crate::io::pages::shared_page::{SharedBufferSlice, SharedRefSlice};
use delegate::delegate;
use std::collections::Bound;
use std::iter::Copied;
use std::ops::{Range, RangeBounds};
use std::slice::Iter;

pub mod kvdata;
pub mod lazy_page;
pub mod ref_page;
pub mod shared_page;

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
    assert!(start <= end, "New start ({start}) should be <= new end ({end})");
    assert!(
      end <= self.end,
      "New end ({end}) should be <= current end ({0})",
      self.end
    );
    start..end
  }
}

pub trait KvDataType: Ord {
  fn partial_eq(&self, other: &[u8]) -> bool;

  fn lt(&self, other: &[u8]) -> bool;
  fn le(&self, other: &[u8]) -> bool;

  fn gt(&self, other: &[u8]) -> bool;
  fn ge(&self, other: &[u8]) -> bool;
}

// TODO: Concept of a 'tx slice and a 'a ref slice

pub trait SubTxSlice<'tx> {
  type TxSlice: KvDataType + 'tx;

  fn sub_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice;
}

pub trait SubRefSlice {
  type RefSlice<'a>: KvDataType + 'a
  where
    Self: 'a;

  fn sub_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefSlice<'a>;
}

pub trait IntoCopiedIterator<'tx> {
  type CopiedIter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a,
    'tx: 'a;
  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a;
}

pub trait RefIntoCopiedIterator {
  type RefCopiedIter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a;
  fn ref_iter_copied<'a>(&'a self) -> Self::RefCopiedIter<'a>;
}

pub trait LolCopiedIter {
  type CopiedIter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a;

  fn lol_copied<'a>(&'a self) -> Self::CopiedIter<'a>;
}

impl LolCopiedIter for [u8] {
  type CopiedIter<'a>
    = Copied<Iter<'a, u8>>
  where
    Self: 'a;

  fn lol_copied<'a>(&'a self) -> Self::CopiedIter<'a> {
    self.into_iter().copied()
  }
}

impl LolCopiedIter for SharedBufferSlice {
  type CopiedIter<'a>
    = Copied<Iter<'a, u8>>
  where
    Self: 'a;

  fn lol_copied<'a>(&'a self) -> Self::CopiedIter<'a> {
    self.inner.as_ref().iter().copied()
  }
}

impl<'p> LolCopiedIter for SharedRefSlice<'p> {
  type CopiedIter<'a>
    = Copied<Iter<'a, u8>>
  where
    Self: 'a;

  fn lol_copied<'a>(&'a self) -> Self::CopiedIter<'a> {
    self.inner.slice.iter().copied()
  }
}
/*
impl<'tx, RD> LolCopiedIter for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
  Self: 'tx,
{
  type CopiedIter<'a>
    = LazySliceIter<'a, RD::PageData, RD>
  where
    Self: 'a,
    Self: 'tx;

  fn lol_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    LazySliceIter::new(self)
  }
}*/

pub trait HasRootPage {
  fn root_page(&self) -> &[u8];
}

pub trait HasHeader: HasRootPage {
  fn page_header(&self) -> &PageHeader {
    bytemuck::from_bytes(&self.root_page()[0..size_of::<PageHeader>()])
  }
}

impl<T> HasHeader for T where T: HasRootPage {}

pub trait TxPage<'tx>: HasHeader + SubTxSlice<'tx> + Clone {}

#[derive(Clone)]
pub struct Page<T> {
  pub(crate) buffer: T,
}

impl<T> HasRootPage for Page<T>
where
  T: HasRootPage,
{
  delegate! {
      to &self.buffer {
          fn root_page(&self) -> &[u8];
      }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  #[test]
  pub fn test() {
    let r1 = 43..127usize;
    let r2 = r1.sub_range(1..10);
    let r3 = r2.sub_range(3..=6);
    println!("{:?} - len: {}", r1, r1.len());
    println!("{:?} - len: {}", r2, r2.len());
    println!("{:?} - len: {}", r3, r3.len());
  }

  #[test]
  #[should_panic]
  pub fn test_panic_order() {
    let r1 = 0..10;
    let r2 = r1.sub_range(7..2);
    println!("{:?} - len: {}", r1, r1.len());
    println!("{:?} - len: {}", r2, r2.len());
  }

  #[test]
  #[should_panic]
  pub fn test_panic_overflow() {
    let r1 = 0..10;
    let r2 = r1.sub_range(7..122);
    println!("{:?} - len: {}", r1, r1.len());
    println!("{:?} - len: {}", r2, r2.len());
  }
}
