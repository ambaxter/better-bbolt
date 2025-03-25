use std::collections::Bound;
use std::ops::{Range, RangeBounds};
use crate::common::errors::PageError;
use crate::common::id::DbPageType;

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


pub trait KvDataType<'tx>: Ord + IntoCopiedIterator<'tx>{
  fn partial_eq(&self, other: &[u8]) -> bool;

  fn lt(&self, other: &[u8]) -> bool;
  fn le(&self, other: &[u8]) -> bool;

  fn gt(&self, other: &[u8]) -> bool;
  fn ge(&self, other: &[u8]) -> bool;

}

pub trait SubSlice<'tx> {
  type OutputSlice: KvDataType<'tx>;

  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::OutputSlice;
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


pub trait HasRootPage {
  fn root_page(&self) -> &[u8];
}

pub trait TxPage<'tx>: HasRootPage + SubSlice<'tx> + Clone {

}

#[derive(Clone)]
pub struct Page<T> {
  pub(crate) buffer: T
}

impl<T> HasRootPage for Page<T> where T: HasRootPage {
  fn root_page(&self) -> &[u8] {
    self.buffer.root_page()
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
