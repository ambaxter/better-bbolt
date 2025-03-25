use crate::common::errors::PageError;
use crate::common::id::OverflowPageId;
use crate::common::page::PageHeader;
use crate::io::{NonContigReader, ReadData};
use crate::pages::impls::{IntoCopiedIterator, SubRange};
use crate::pages::{HasHeader, Page};
use delegate::delegate;
use error_stack::ResultExt;
use std::ops::{Deref, Index, Range, RangeBounds};
use triomphe::Arc;

pub trait HasRootPage {
  fn root_page(&self) -> &[u8];
}

pub trait TxPageSlice<'tx>:
  Ord + PartialEq<[u8]> + PartialOrd<[u8]> + IntoCopiedIterator<'tx>
{
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

// TODO: [u8] wrapper because we have to?
pub trait TxPage<'tx>: AsRef<[u8]> + Clone {
  type TxSlice: TxPageSlice<'tx>;

  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice;
}

pub struct LazyPage<T, R> {
  root: Page<T>,
  io: Arc<R>,
}

impl<T, R> Clone for LazyPage<T, R>
where
  T: Clone,
{
  fn clone(&self) -> Self {
    LazyPage {
      root: self.root.clone(),
      io: self.io.clone(),
    }
  }
}

impl<'tx, RD> HasRootPage for LazyPage<RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  delegate! {
      to &self.root {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, RD> HasHeader for LazyPage<RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  delegate! {
      to &self.root {
          fn page_header(&self) -> &PageHeader;
      }
  }
}

impl<'tx, RD> LazyPage<RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  pub fn len(&self) -> usize {
    self.root.root_page().len() * (1 + self.root.page_header().get_overflow() as usize)
  }
}

#[derive(Clone)]
pub struct LazySlice<T, RD> {
  pub(crate) page: LazyPage<T, RD>,
  pub(crate) range: Range<usize>,
}

impl<'tx, RD> LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  pub fn new<R: RangeBounds<usize>>(page: LazyPage<RD::PageData, RD>, range: R) -> Self {
    let range = (0..page.len()).sub_range(range);
    Self { page, range }
  }

  fn read_overflow_page(
    &self, idx: usize,
  ) -> crate::Result<LazyPageBytes<RD::PageData>, PageError> {
    let page_size = self.page.root_page().len();
    let overflow_index = (idx / page_size) as u32;
    let header = self.page.page_header();
    let page_overflow = header.get_overflow();
    let page_id = header.overflow_page_id().expect("overflow page id");
    assert!(overflow_index <= page_overflow);
    if overflow_index == 0 {
      Ok(LazyPageBytes {
        bytes: self.page.root.buffer.clone(),
        overflow_index,
      })
    } else {
      match page_id {
        OverflowPageId::Freelist(page_id) => {
          self.page.io.read_freelist_overflow(page_id, overflow_index)
        }
        OverflowPageId::Node(page_id) => self.page.io.read_node_overflow(page_id, overflow_index),
      }
      .map(|bytes| LazyPageBytes {
        bytes,
        overflow_index,
      })
      .change_context(PageError::OverflowReadError(page_id, overflow_index))
    }
  }

  fn next_overflow_page(
    &self, idx: usize, range_end: usize,
  ) -> crate::Result<LazyPageBytesIter<RD::PageData>, PageError> {
    self.read_overflow_page(idx).map(|bytes| {
      let page_size = bytes.as_ref().len();
      let next_page_idx = bytes.page_index();
      let back_page_idx = (range_end / page_size) as u32;
      let len = if back_page_idx == next_page_idx {
        range_end % page_size
      } else {
        page_size
      };
      LazyPageBytesIter {
        bytes,
        range: 0..len,
      }
    })
  }

  fn next_back_overflow_page(
    &self, idx: usize, rane_start: usize,
  ) -> crate::Result<LazyPageBytesIter<RD::PageData>, PageError> {
    self.read_overflow_page(idx).map(|bytes| {
      let page_size = bytes.as_ref().len();
      let back_page_idx = bytes.page_index();
      let next_page_idx = (rane_start / page_size) as u32;
      let start = if back_page_idx == next_page_idx {
        rane_start % page_size
      } else {
        0
      };
      LazyPageBytesIter {
        bytes,
        range: start..page_size,
      }
    })
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.range.len()
  }
}

#[derive(Clone)]
struct LazyPageBytes<T> {
  bytes: T,
  overflow_index: u32,
}

impl<T> LazyPageBytes<T> {
  #[inline]
  fn page_index(&self) -> u32 {
    self.overflow_index
  }
}

impl<'tx, T> Index<usize> for LazyPageBytes<T>
where
  T: TxPage<'tx>,
{
  type Output = u8;

  fn index(&self, index: usize) -> &Self::Output {
    &self.as_ref()[index]
  }
}

impl<'tx, T> AsRef<[u8]> for LazyPageBytes<T>
where
  T: TxPage<'tx>,
{
  #[inline]
  fn as_ref(&self) -> &[u8] {
    self.bytes.as_ref()
  }
}

#[derive(Clone)]
struct LazyPageBytesIter<T> {
  bytes: LazyPageBytes<T>,
  range: Range<usize>,
}

impl<T> LazyPageBytesIter<T> {
  #[inline]
  fn page_index(&self) -> u32 {
    self.bytes.page_index()
  }
}

impl<'tx, T> Iterator for LazyPageBytesIter<T>
where
  T: TxPage<'tx>,
{
  type Item = u8;

  fn next(&mut self) -> Option<Self::Item> {
    self
      .range
      .next()
      .map(|idx| self.bytes.as_ref().get(idx).copied())
      .flatten()
  }
}

impl<'tx, T> DoubleEndedIterator for LazyPageBytesIter<T>
where
  T: TxPage<'tx>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .range
      .next_back()
      .map(|idx| self.bytes.as_ref().get(idx).copied())
      .flatten()
  }
}

impl<'tx, T> ExactSizeIterator for LazyPageBytesIter<T>
where
  T: TxPage<'tx>,
{
  fn len(&self) -> usize {
    self.range.len()
  }
}

pub struct LazySliceIter<'a, T, R> {
  slice: &'a LazySlice<T, R>,
  range: Range<usize>,
  next: LazyPageBytesIter<T>,
  next_back: LazyPageBytesIter<T>,
}

impl<'a, 'tx: 'a, RD> LazySliceIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  pub fn new(slice: &'a LazySlice<RD::PageData, RD>) -> Self {
    let range = slice.range.clone();
    let next = slice
      .next_overflow_page(range.start, range.end)
      .expect("next overflow read error");
    let next_back = slice
      .next_back_overflow_page(range.end, range.start)
      .expect("next_back overflow read error");
    LazySliceIter {
      slice,
      range,
      next,
      next_back,
    }
  }

  fn next_overflow_page(
    &self, idx: usize,
  ) -> crate::Result<LazyPageBytesIter<RD::PageData>, PageError> {
    self.slice.next_overflow_page(idx, self.range.end)
  }

  fn next_back_overflow_page(
    &self, idx: usize,
  ) -> crate::Result<LazyPageBytesIter<RD::PageData>, PageError> {
    self.slice.next_back_overflow_page(idx, self.range.start)
  }
}

impl<'a, 'tx, RD> Iterator for LazySliceIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  type Item = u8;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(idx) = self.range.next() {
      return if let Some(next) = self.next.next() {
        Some(next)
      } else {
        let next = self
          .next_overflow_page(idx)
          .expect("next overflow read error");
        self.next = next;
        self.next.next()
      };
    }
    None
  }
}

impl<'a, 'tx, RD> DoubleEndedIterator for LazySliceIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    if let Some(idx) = self.range.next_back() {
      return if let Some(next) = self.next_back.next_back() {
        Some(next)
      } else {
        let back = self
          .next_back_overflow_page(idx)
          .expect("back overflow read error");
        self.next_back = back;
        self.next_back.next_back()
      };
    }
    None
  }
}

impl<'a, 'tx, RD> ExactSizeIterator for LazySliceIter<'a, RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  #[inline]
  fn len(&self) -> usize {
    self.range.len()
  }
}

impl<'tx, RD> PartialEq for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn eq(&self, other: &Self) -> bool {
    self.iter_copied().eq(other.iter_copied())
  }
}

impl<'tx, RD> Eq for LazySlice<RD::PageData, RD> where RD: NonContigReader<'tx> + 'tx {}

impl<'tx, RD> PartialOrd for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self.iter_copied().partial_cmp(other.iter_copied())
  }
}

impl<'tx, RD> Ord for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.iter_copied().cmp(other.iter_copied())
  }
}
