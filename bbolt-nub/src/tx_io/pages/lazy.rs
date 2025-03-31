use crate::common::errors::{DiskReadError, PageError};
use crate::common::id::OverflowPageId;
use crate::tx_io::TxSlot;
use crate::tx_io::backends::ReadLazyIO;
use crate::tx_io::bytes::TxBytes;
use crate::tx_io::pages::{
  GetKvRefSlice, GetKvTxSlice, Page, ReadLazyPageIO, RefIntoCopiedIter, SubRange, TxPage,
};
use error_stack::ResultExt;
use std::cmp::Ordering;
use std::ops::{Range, RangeBounds};
use triomphe::Arc;

pub struct LazyPage<'tx, L: ReadLazyPageIO<'tx>> {
  tx: TxSlot<'tx>,
  root: L::PageBytes,
  r: Option<&'tx L>,
}

impl<'tx, L: ReadLazyPageIO<'tx>> Clone for LazyPage<'tx, L> {
  fn clone(&self) -> Self {
    LazyPage {
      tx: self.tx,
      root: self.root.clone(),
      r: self.r,
    }
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> LazyPage<'tx, L> {
  pub fn len(&self) -> usize {
    self.root_page().len() * (self.page_header().get_overflow() + 1) as usize
  }

  pub fn read_overflow_page(&self, overflow_index: u32) -> crate::Result<L::PageBytes, PageError> {
    let header = self.page_header();
    let overflow_count = header.get_overflow();
    assert!(overflow_index <= overflow_count);
    if overflow_index == 0 {
      Ok(self.root.clone())
    } else {
      let page_id = header.overflow_page_id().expect("overflow page id");
      match page_id {
        OverflowPageId::Freelist(page_id) => self
          .r
          .unwrap()
          .read_freelist_overflow(page_id, overflow_index),
        OverflowPageId::Node(page_id) => {
          self.r.unwrap().read_node_overflow(page_id, overflow_index)
        }
      }
      .change_context(PageError::OverflowReadError(page_id, overflow_index))
    }
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> Page for LazyPage<'tx, L> {
  #[inline]
  fn root_page(&self) -> &[u8] {
    self.root.as_ref()
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> GetKvRefSlice for LazyPage<'tx, L> {
  type RefKv<'a>
    = LazyRefSlice<'a, 'tx, L>
  where
    Self: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    let range = (0..self.len()).sub_range(range);
    LazyRefSlice { page: self, range }
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> GetKvTxSlice<'tx> for LazyPage<'tx, L> {
  type TxKv = LazyTxSlice<'tx, L>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = (0..self.len()).sub_range(range);
    LazyTxSlice {
      page: self.clone(),
      range,
    }
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> TxPage<'tx> for LazyPage<'tx, L> {}

#[derive(Clone)]
pub struct LazyIter<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> {
  page: &'a LazyPage<'tx, L>,
  range: Range<usize>,
  next_overflow_index: u32,
  next_page: L::PageBytes,
  next_back_overflow_index: u32,
  next_back_page: L::PageBytes,
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> LazyIter<'a, 'tx, L> {
  pub fn new<R: RangeBounds<usize>>(page: &'a LazyPage<'tx, L>, range: R) -> LazyIter<'a, 'tx, L> {
    let page_size = page.root_page().len();
    let overflow_count = page.page_header().get_overflow();
    let range = (0..page.len()).sub_range(range);
    let next_overflow_index = (range.start / page_size) as u32;
    let next_page = page
      .read_overflow_page(next_overflow_index)
      .expect("unable to read next overflow page");
    let next_back_overflow_index = (range.end / page_size) as u32;
    let next_back_page = page
      .read_overflow_page(next_back_overflow_index)
      .expect("unable to read next_back overflow page");

    LazyIter {
      page,
      range,
      next_overflow_index,
      next_page,
      next_back_overflow_index,
      next_back_page,
    }
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> Iterator for LazyIter<'a, 'tx, L> {
  type Item = u8;
  fn next(&mut self) -> Option<Self::Item> {
    let index = self.range.next()?;
    let page_size = self.page.root_page().len();
    let next_overflow_index = (index / page_size) as u32;
    if next_overflow_index != self.next_overflow_index {
      self.next_page = self
        .page
        .read_overflow_page(next_overflow_index)
        .expect("unable to read next overflow page");
      self.next_overflow_index = next_overflow_index;
    }
    let page_index = index % page_size;
    self.next_page.as_ref().get(page_index).copied()
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> DoubleEndedIterator for LazyIter<'a, 'tx, L> {
  fn next_back(&mut self) -> Option<Self::Item> {
    let index = self.range.next_back()?;
    let page_size = self.page.root_page().len();
    let next_back_overflow_index = (index / page_size) as u32;
    if next_back_overflow_index != self.next_back_overflow_index {
      self.next_back_page = self
        .page
        .read_overflow_page(next_back_overflow_index)
        .expect("unable to read next overflow page");
      self.next_back_overflow_index = next_back_overflow_index;
    }
    let page_index = index % page_size;
    self.next_back_page.as_ref().get(page_index).copied()
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> ExactSizeIterator for LazyIter<'a, 'tx, L> {
  #[inline]
  fn len(&self) -> usize {
    self.range.len()
  }
}

#[derive(Clone)]
pub struct LazyRefSlice<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> {
  page: &'a LazyPage<'tx, L>,
  range: Range<usize>,
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> PartialOrd for LazyRefSlice<'a, 'tx, L> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self
      .ref_into_copied_iter()
      .partial_cmp(other.ref_into_copied_iter())
  }

  fn lt(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().lt(other.ref_into_copied_iter())
  }

  fn le(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().le(other.ref_into_copied_iter())
  }

  fn gt(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().gt(other.ref_into_copied_iter())
  }

  fn ge(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().ge(other.ref_into_copied_iter())
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> Ord for LazyRefSlice<'a, 'tx, L> {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .ref_into_copied_iter()
      .cmp(other.ref_into_copied_iter())
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> PartialEq for LazyRefSlice<'a, 'tx, L> {
  fn eq(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().eq(other.ref_into_copied_iter())
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> Eq for LazyRefSlice<'a, 'tx, L> {}

impl<'p, 'tx: 'p, L: ReadLazyPageIO<'tx>> GetKvRefSlice for LazyRefSlice<'p, 'tx, L> {
  type RefKv<'a>
    = LazyRefSlice<'a, 'tx, L>
  where
    Self: 'a,
    'p: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    LazyRefSlice {
      page: self.page,
      range: self.range.sub_range(range),
    }
  }
}

impl<'p, 'tx: 'p, L: ReadLazyPageIO<'tx>> RefIntoCopiedIter for LazyRefSlice<'p, 'tx, L> {
  type Iter<'a>
    = LazyIter<'a, 'tx, L>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    LazyIter::new(self.page, self.range.clone())
  }
}

#[derive(Clone)]
pub struct LazyTxSlice<'tx, L: ReadLazyPageIO<'tx>> {
  page: LazyPage<'tx, L>,
  range: Range<usize>,
}

impl<'tx, L: ReadLazyPageIO<'tx>> PartialOrd for LazyTxSlice<'tx, L> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self
      .ref_into_copied_iter()
      .partial_cmp(other.ref_into_copied_iter())
  }

  fn lt(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().lt(other.ref_into_copied_iter())
  }

  fn le(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().le(other.ref_into_copied_iter())
  }

  fn gt(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().gt(other.ref_into_copied_iter())
  }

  fn ge(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().ge(other.ref_into_copied_iter())
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> Ord for LazyTxSlice<'tx, L> {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .ref_into_copied_iter()
      .cmp(other.ref_into_copied_iter())
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> PartialEq for LazyTxSlice<'tx, L> {
  fn eq(&self, other: &Self) -> bool {
    self.ref_into_copied_iter().eq(other.ref_into_copied_iter())
  }
}
impl<'tx, L: ReadLazyPageIO<'tx>> Eq for LazyTxSlice<'tx, L> {}

impl<'tx, L: ReadLazyPageIO<'tx>> RefIntoCopiedIter for LazyTxSlice<'tx, L> {
  type Iter<'a>
    = LazyIter<'a, 'tx, L>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    LazyIter::new(&self.page, self.range.clone())
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> GetKvRefSlice for LazyTxSlice<'tx, L> {
  type RefKv<'a>
    = LazyRefSlice<'a, 'tx, L>
  where
    Self: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    let range = self.range.sub_range(range);
    LazyRefSlice {
      page: &self.page,
      range,
    }
  }
}

impl<'tx, L: ReadLazyPageIO<'tx>> GetKvTxSlice<'tx> for LazyTxSlice<'tx, L> {
  type TxKv = LazyTxSlice<'tx, L>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = self.range.sub_range(range);
    LazyTxSlice {
      page: self.page.clone(),
      range,
    }
  }
}
