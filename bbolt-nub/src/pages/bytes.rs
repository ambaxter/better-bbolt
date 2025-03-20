use crate::common::errors::PageError;
use crate::common::id::OverflowPageId;
use crate::common::page::PageHeader;
use crate::io::{ReadData, ReadOverflow};
use crate::pages::{HasHeader, Page};
use delegate::delegate;
use error_stack::ResultExt;
use std::ops::{Index, Range, RangeBounds};
use triomphe::Arc;

pub trait HasRootPage {
  fn root_page(&self) -> &[u8];
}

pub trait IntoCopiedIterator {
  fn into_copied_iter(self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator;
}

pub trait ByteSlice<'a>:
  Ord + PartialEq<&'a [u8]> + PartialOrd<&'a [u8]> + IntoCopiedIterator
{
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

impl<'a> IntoCopiedIterator for &'a [u8] {
  fn into_copied_iter(self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator {
    self.iter().cloned()
  }
}

impl<'a> ByteSlice<'a> for &'a [u8] {
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

pub trait TxPageSlice<'tx>:
  Ord + for<'a> PartialEq<&'a [u8]> + for<'a> PartialOrd<&'a [u8]> + IntoCopiedIterator
{
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

pub trait TxPage<'tx>: Clone + AsRef<[u8]> {
  type TxSlice: TxPageSlice<'tx>;

  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice;
}

pub trait PageBytes: Clone + AsRef<[u8]> {
  type Subslice<'s>: ByteSlice<'s>
  where
    Self: 's;

  fn subslice<'s, R: RangeBounds<usize>>(&'s self, range: R) -> Self::Subslice<'s>
  where
    Self: 's;
}

impl<T> PageBytes for T
where
  T: AsRef<[u8]> + Clone,
{
  type Subslice<'s>
    = &'s [u8]
  where
    Self: 's;

  fn subslice<'s, R: RangeBounds<usize>>(&'s self, range: R) -> Self::Subslice<'s>
  where
    Self: 's,
  {
    &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
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

impl<'tx, R> HasRootPage for LazyPage<R::Output, R>
where
  R: ReadOverflow<'tx>,
{
  delegate! {
      to &self.root {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, R> HasHeader for LazyPage<R::Output, R>
where
  R: ReadOverflow<'tx>,
{
  delegate! {
      to &self.root {
          fn page_header(&self) -> &PageHeader;
      }
  }
}

#[derive(Clone)]
pub struct LazySlice<T, R> {
  page: LazyPage<T, R>,
  range: Range<usize>,
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
struct LazyPageIter<T> {
  bytes: LazyPageBytes<T>,
  range: Range<usize>,
}

impl<T> LazyPageIter<T> {
  #[inline]
  fn page_index(&self) -> u32 {
    self.bytes.page_index()
  }
}

impl<'tx, T> Iterator for LazyPageIter<T>
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

impl<'tx, T> DoubleEndedIterator for LazyPageIter<T>
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

impl<'tx, T> ExactSizeIterator for LazyPageIter<T>
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
  next: LazyPageIter<T>,
  back: LazyPageIter<T>,
}

impl<'a, 'tx, R> LazySliceIter<'a, R::Output, R>
where
  R: ReadOverflow<'tx>,
{
  fn read_overflow_page(&self, idx: usize) -> crate::Result<LazyPageBytes<R::Output>, PageError> {
    let page_size = self.slice.page.root_page().len();
    let overflow_index = (idx / page_size) as u32;
    let header = self.slice.page.page_header();
    let page_overflow = header.get_overflow();
    let page_id = header.overflow_page_id().expect("overflow page id");
    assert!(overflow_index <= page_overflow);
    if overflow_index == 0 {
      Ok(LazyPageBytes {
        bytes: self.slice.page.root.buffer.clone(),
        overflow_index,
      })
    } else {
      match page_id {
        OverflowPageId::Freelist(page_id) => self
          .slice
          .page
          .io
          .read_freelist_overflow(page_id, overflow_index),
        OverflowPageId::Node(page_id) => self
          .slice
          .page
          .io
          .read_node_overflow(page_id, overflow_index),
      }
      .map(|bytes| LazyPageBytes {
        bytes,
        overflow_index,
      })
      .change_context(PageError::OverflowReadError(page_id, overflow_index))
    }
  }

  fn next_overflow_page(&self, idx: usize) -> crate::Result<LazyPageIter<R::Output>, PageError> {
    self.read_overflow_page(idx).map(|bytes| {
      let page_size = bytes.as_ref().len();
      let next_page_idx = bytes.page_index();
      let back_page_idx = (self.range.end / page_size) as u32;
      let len = if back_page_idx == next_page_idx {
        self.range.end % page_size
      } else {
        page_size
      };
      LazyPageIter {
        bytes,
        range: 0..len,
      }
    })
  }

  fn next_back_overflow_page(
    &self, idx: usize,
  ) -> crate::Result<LazyPageIter<R::Output>, PageError> {
    self.read_overflow_page(idx).map(|bytes| {
      let page_size = bytes.as_ref().len();
      let back_page_idx = bytes.page_index();
      let next_page_idx = (self.range.start / page_size) as u32;
      let start = if back_page_idx == next_page_idx {
        self.range.start % page_size
      } else {
        0
      };
      LazyPageIter {
        bytes,
        range: start..page_size,
      }
    })
  }
}

impl<'a, 'tx, R> Iterator for LazySliceIter<'a, R::Output, R>
where
  R: ReadOverflow<'tx>,
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

impl<'a, 'tx, R> DoubleEndedIterator for LazySliceIter<'a, R::Output, R>
where
  R: ReadOverflow<'tx>,
{
  fn next_back(&mut self) -> Option<Self::Item> {
    if let Some(idx) = self.range.next_back() {
      return if let Some(next) = self.back.next_back() {
        Some(next)
      } else {
        let back = self
          .next_back_overflow_page(idx)
          .expect("back overflow read error");
        self.back = back;
        self.back.next_back()
      };
    }
    None
  }
}

impl<'a, 'tx, R> ExactSizeIterator for LazySliceIter<'a, R::Output, R>
where
  R: ReadOverflow<'tx>,
{
  #[inline]
  fn len(&self) -> usize {
    self.range.len()
  }
}
