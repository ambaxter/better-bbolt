use crate::io::NonContigReader;
use crate::pages::bytes::{LazySlice, LazySliceIter};
use std::iter::Copied;
use std::ops::RangeBounds;


pub trait IntoCopiedIterator<'tx>
where
  Self: 'tx,
{
  type CopiedIter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a,
    'tx: 'a;
  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a;
}

impl<'tx> IntoCopiedIterator<'tx> for &'tx [u8] {
  type CopiedIter<'a>
  = Copied<std::slice::Iter<'a, u8>>
  where
    Self: 'a,
    'tx: 'a;

  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    self.iter().copied()
  }
}

impl<'tx, R> IntoCopiedIterator<'tx> for LazySlice<R::PageData, R>
where
  R: NonContigReader<'tx> + 'tx,
{
  type CopiedIter<'a>
  = LazySliceIter<'a, R::PageData, R>
  where
    Self: 'a,
    'tx: 'a;

  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    LazySliceIter::new(self)
  }
}


pub trait KvData<'tx>: Ord + IntoCopiedIterator<'tx> {
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
