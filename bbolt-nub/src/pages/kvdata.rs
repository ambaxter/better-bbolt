use crate::io::{NonContigReader, ReadData};
use crate::pages::bytes::{LazySlice, LazySliceIter};
use std::cmp::Ordering;
use std::collections::Bound;
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

pub trait KvDataType<'tx>: Ord + IntoCopiedIterator<'tx> {
  fn partial_eq(&self, other: &[u8]) -> bool;

  fn lt(&self, other: &[u8]) -> bool;
  fn le(&self, other: &[u8]) -> bool;

  fn gt(&self, other: &[u8]) -> bool;
  fn ge(&self, other: &[u8]) -> bool;

  fn slice_index<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

impl<'a> KvDataType<'a> for &'a [u8] {
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

impl<'tx, Q> KvDataType<'tx> for LazySlice<Q::PageData, Q>
where
  Q: NonContigReader<'tx> + 'tx,
{
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.iter_copied().eq(other.iter_copied())
  }

  fn lt(&self, other: &[u8]) -> bool {
    self.iter_copied().lt(other.iter_copied())
  }

  fn le(&self, other: &[u8]) -> bool {
    self.iter_copied().le(other.iter_copied())
  }

  fn gt(&self, other: &[u8]) -> bool {
    self.iter_copied().gt(other.iter_copied())
  }

  fn ge(&self, other: &[u8]) -> bool {
    self.iter_copied().ge(other.iter_copied())
  }

  fn slice_index<R: RangeBounds<usize>>(&self, range: R) -> Self {
    todo!()
  }
}

pub enum KvData<'tx, T, R> {
  Slice(&'tx [u8]),
  LazySlice(LazySlice<T, R>),
}

impl<'tx, R> IntoCopiedIterator<'tx> for KvData<'tx, R::PageData, R>
where
  R: NonContigReader<'tx> + 'tx,
{
  type CopiedIter<'a>
    = KvDataIter<'a, R::PageData, R>
  where
    Self: 'a,
    'tx: 'a;

  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    match self {
      KvData::Slice(slice) => KvDataIter::Slice(slice.iter_copied()),
      KvData::LazySlice(slice) => KvDataIter::LazySlice(slice.iter_copied()),
    }
  }
}

pub enum KvDataIter<'a, T, R> {
  Slice(Copied<std::slice::Iter<'a, u8>>),
  LazySlice(LazySliceIter<'a, T, R>),
}

impl<'a, 'tx, R> Iterator for KvDataIter<'a, R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  type Item = u8;

  fn next(&mut self) -> Option<Self::Item> {
    match self {
      KvDataIter::Slice(iter) => iter.next(),
      KvDataIter::LazySlice(iter) => iter.next(),
    }
  }
}

impl<'a, 'tx, R> DoubleEndedIterator for KvDataIter<'a, R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    match self {
      KvDataIter::Slice(iter) => iter.next_back(),
      KvDataIter::LazySlice(iter) => iter.next_back(),
    }
  }
}

impl<'a, 'tx, R> ExactSizeIterator for KvDataIter<'a, R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  #[inline]
  fn len(&self) -> usize {
    match self {
      KvDataIter::Slice(iter) => iter.len(),
      KvDataIter::LazySlice(iter) => iter.len(),
    }
  }
}

impl<'a, 'tx, R> PartialEq for KvData<'a, R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  fn eq(&self, other: &Self) -> bool {
    self.iter_copied().eq(other.iter_copied())
  }
}

impl<'a, 'tx, R> Eq for KvData<'a, R::PageData, R> where R: NonContigReader<'tx> {}

impl<'a, 'tx, R> PartialOrd for KvData<'a, R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.iter_copied().partial_cmp(other.iter_copied())
  }

  fn lt(&self, other: &Self) -> bool {
    self.iter_copied().lt(other.iter_copied())
  }

  fn gt(&self, other: &Self) -> bool {
    self.iter_copied().gt(other.iter_copied())
  }

  fn ge(&self, other: &Self) -> bool {
    self.iter_copied().ge(other.iter_copied())
  }
}

impl<'a, 'tx, R> Ord for KvData<'a, R::PageData, R>
where
  R: NonContigReader<'tx>,
{
  fn cmp(&self, other: &Self) -> Ordering {
    self.iter_copied().cmp(other.iter_copied())
  }
}
