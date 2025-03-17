use crate::io::ReadData;
use crate::pages::{HasHeader, Page};
use delegate::delegate;
use std::ops::{Range, RangeBounds};
use triomphe::Arc;

pub trait HasRootPage {
  fn root_page(&self) -> &[u8];
}

pub trait IntoCopiedIterator {
  fn into_cloned_iter(self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator;
}

pub trait ByteSlice<'a>:
  Ord + PartialEq<&'a [u8]> + PartialOrd<&'a [u8]> + IntoCopiedIterator
{
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

impl<'a> IntoCopiedIterator for &'a [u8] {
  fn into_cloned_iter(self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator {
    self.iter().cloned()
  }
}

impl<'a> ByteSlice<'a> for &'a [u8] {
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
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

#[derive(Clone)]
pub struct LazyPage<T: PageBytes, R: ReadData> {
  root: Page<T>,
  io: Arc<R>,
}

impl<T: PageBytes, R: ReadData> HasRootPage for LazyPage<T, R> {
  delegate! {
      to &self.root {
          fn root_page(&self) -> &[u8];
      }
  }
}

#[derive(Clone)]
pub struct LazySlice<'a, T: PageBytes, R: ReadData> {
  page: &'a LazyPage<T, R>,
  range: Range<usize>,
}

#[derive(Clone)]
enum LazyBytes<'a, T: PageBytes> {
  Root(&'a Page<T>),
  DataBytes { page: T, overflow_index: u32 },
}

impl<'a, T: PageBytes> AsRef<[u8]> for LazyBytes<'a, T> {
  fn as_ref(&self) -> &[u8] {
    match self {
      LazyBytes::Root(root) => root.root_page(),
      LazyBytes::DataBytes {
        page,
        overflow_index: _,
      } => page.as_ref(),
    }
  }
}

#[derive(Clone)]
struct LazyIter<'a, T: PageBytes> {
  bytes: LazyBytes<'a, T>,
  range: Range<usize>,
}

impl<'a, T: PageBytes> LazyIter<'a, T> {
  fn next_pos(&self) -> usize {
    let page_offset = match &self.bytes {
      LazyBytes::Root(page) => page.root_page().len() * 0,
      LazyBytes::DataBytes {
        page,
        overflow_index,
      } => page.as_ref().len() * *overflow_index as usize,
    };
    page_offset + self.range.start
  }

  fn back_pos(&self) -> usize {
    let page_offset = match &self.bytes {
      LazyBytes::Root(page) => page.root_page().len() * 0,
      LazyBytes::DataBytes {
        page,
        overflow_index,
      } => page.as_ref().len() * *overflow_index as usize,
    };
    page_offset + self.range.end
  }

  fn page_index(&self) -> u32 {
    match &self.bytes {
      LazyBytes::Root(_) => 0,
      LazyBytes::DataBytes {
        page: _page,
        overflow_index,
      } => *overflow_index,
    }
  }
}

impl<'a, T: PageBytes> Iterator for LazyIter<'a, T> {
  type Item = u8;

  fn next(&mut self) -> Option<Self::Item> {
    self
      .range
      .next()
      .map(|idx| self.bytes.as_ref().get(idx).copied())
      .flatten()
  }
}

impl<'a, T: PageBytes> DoubleEndedIterator for LazyIter<'a, T> {
  fn next_back(&mut self) -> Option<Self::Item> {
    self
      .range
      .next_back()
      .map(|idx| self.bytes.as_ref().get(idx).copied())
      .flatten()
  }
}

impl<'a, T: PageBytes> ExactSizeIterator for LazyIter<'a, T> {
  fn len(&self) -> usize {
    self.range.len()
  }
}

pub struct LazySliceIter<'a, T: PageBytes, R: ReadData> {
  slice: LazySlice<'a, T, R>,
  range: Range<usize>,
  next: LazyIter<'a, T>,
  back: LazyIter<'a, T>,
}

impl<'a, T: PageBytes, R: ReadData> Iterator for LazySliceIter<'a, T, R> {
  type Item = u8;

  fn next(&mut self) -> Option<Self::Item> {
    if let Some(idx) = self.range.next() {
      if let Some(next) = self.next.next() {
        return Some(next);
      } else {
        let page_size = self.next.bytes.as_ref().len();
        let next_page_idx = idx / page_size;
        let page_overflow = self.slice.page.root.page_header().get_overflow();
        let db_page_id = self.slice.page.root.page_header().db_page_id();

        unimplemented!()
        /*if next_page_idx <= self.slice.page.root.page_header().get_overflow() as usize {
          match db_page_id {
            // Now we are at the point of needing to figure out how
            // this we translate page ids to disk page ids
            DbPageTypes::Node(id) => id + page_overflow,
            DbPageTypes::Freelist(id) => id + page_overflow,
            _ => unreachable!(),
          }
        }*/
      }
    }
    None
  }
}
