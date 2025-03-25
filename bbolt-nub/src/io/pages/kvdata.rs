use crate::io::pages::IntoCopiedIterator;
use crate::io::pages::shared_page::SharedBufferSlice;
use crate::io::{NonContigReader, ReadData};
use std::cmp::Ordering;
use std::collections::Bound;
use std::iter::Copied;
use std::ops::RangeBounds;
use crate::io::pages::lazy_page::{LazySlice, LazySliceIter};

pub enum KvData<'tx, T, RD> {
  Slice(&'tx [u8]),
  SharedSlice(SharedBufferSlice),
  LazySlice(LazySlice<T, RD>),
}

impl<'tx, RD> IntoCopiedIterator<'tx> for KvData<'tx, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  type CopiedIter<'a>
    = KvDataIter<'a, RD::PageData, RD>
  where
    Self: 'a,
    'tx: 'a;

  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    match self {
      KvData::Slice(slice) => KvDataIter::Slice(slice.iter_copied()),
      KvData::SharedSlice(slice) => KvDataIter::Slice(slice.iter_copied()),
      KvData::LazySlice(slice) => KvDataIter::LazySlice(slice.iter_copied()),
    }
  }
}

pub enum KvDataIter<'a, T, RD> {
  Slice(Copied<std::slice::Iter<'a, u8>>),
  LazySlice(LazySliceIter<'a, T, RD>),
}

impl<'a, 'tx, RD> Iterator for KvDataIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  type Item = u8;

  fn next(&mut self) -> Option<Self::Item> {
    match self {
      KvDataIter::Slice(iter) => iter.next(),
      KvDataIter::LazySlice(iter) => iter.next(),
    }
  }
}

impl<'a, 'tx, RD> DoubleEndedIterator for KvDataIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    match self {
      KvDataIter::Slice(iter) => iter.next_back(),
      KvDataIter::LazySlice(iter) => iter.next_back(),
    }
  }
}

impl<'a, 'tx, RD> ExactSizeIterator for KvDataIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  #[inline]
  fn len(&self) -> usize {
    match self {
      KvDataIter::Slice(iter) => iter.len(),
      KvDataIter::LazySlice(iter) => iter.len(),
    }
  }
}

impl<'tx, RD> PartialEq for KvData<'tx, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn eq(&self, other: &Self) -> bool {
    self.iter_copied().eq(other.iter_copied())
  }
}

impl<'tx, RD> Eq for KvData<'tx, RD::PageData, RD> where RD: NonContigReader<'tx> + 'tx {}

impl<'tx, RD> PartialOrd for KvData<'tx, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
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

impl<'tx, RD> Ord for KvData<'tx, RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn cmp(&self, other: &Self) -> Ordering {
    self.iter_copied().cmp(other.iter_copied())
  }
}
