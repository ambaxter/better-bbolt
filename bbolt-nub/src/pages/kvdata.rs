use crate::io::NonContigReader;
use crate::pages::bytes::{LazySlice, LazySliceIter};
use std::ops::RangeBounds;

pub trait IntoCopiedIterator {
  fn iter_copied(&self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator;
}

impl<'a> IntoCopiedIterator for &'a [u8] {
  fn iter_copied(&self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator {
    self.iter().cloned()
  }
}

pub trait KvData<'tx>: Ord + IntoCopiedIterator {
  fn partial_eq(&self, other: &[u8]) -> bool;

  fn lt(&self, other: &[u8]) -> bool;
  fn le(&self, other: &[u8]) -> bool;

  fn gt(&self, other: &[u8]) -> bool;
  fn ge(&self, other: &[u8]) -> bool;

  fn slice_index<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

impl<'a> KvData<'a> for &'a [u8] {
  #[inline]
  fn partial_eq(&self, other: &[u8]) -> bool {
    PartialEq::eq(*self, other)
  }

  #[inline]
  fn lt(&self, other: &[u8]) -> bool {
    PartialOrd::lt(*self, other)
  }

  #[inline]
  fn le(&self, other: &[u8]) -> bool {
    PartialOrd::le(*self, other)
  }

  #[inline]
  fn gt(&self, other: &[u8]) -> bool {
    PartialOrd::gt(*self, other)
  }

  #[inline]
  fn ge(&self, other: &[u8]) -> bool {
    PartialOrd::ge(*self, other)
  }

  fn slice_index<R: RangeBounds<usize>>(&self, range: R) -> Self {
    let (start, end) = (range.start_bound().cloned(), range.end_bound().cloned());
    &self[(start, end)]
  }
}

impl<'tx, R> IntoCopiedIterator for LazySlice<R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  fn iter_copied(&self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator {
    LazySliceIter::new(self)
  }
}
