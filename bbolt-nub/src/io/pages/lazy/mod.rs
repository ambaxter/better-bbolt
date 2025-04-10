use crate::common::errors::{OpsError, PageError};
use crate::common::id::OverflowPageId;
use crate::io::TxSlot;
use crate::io::bytes::shared_bytes::SharedTxBytes;
use crate::io::ops::{
  GetKvRefSlice, GetKvTxSlice, KvDataType, KvEq, KvOrd, RefIntoCopiedIter, RefIntoTryBuf, SubRange,
  TryBuf, TryGet, TryHash, TryPartialEq, TryPartialOrd,
};
use crate::io::pages::lazy::ref_slice::LazyRefTryBuf;
use crate::io::pages::lazy::tx_slice::LazyTxSlice;
use crate::io::pages::{Page, TxPageType, TxReadLazyPageIO, TxReadPageIO};
use error_stack::ResultExt;
use ref_slice::LazyRefSlice;
use std::hash;
use std::ops::{Deref, Range, RangeBounds};

pub mod ref_slice;
pub mod tx_slice;

pub struct LazyPage<'tx, L: TxReadLazyPageIO<'tx>> {
  tx: TxSlot<'tx>,
  root: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  r: Option<&'tx L>,
}

unsafe impl<'tx, L: TxReadLazyPageIO<'tx>> Send for LazyPage<'tx, L> {}
unsafe impl<'tx, L: TxReadLazyPageIO<'tx>> Sync for LazyPage<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> Clone for LazyPage<'tx, L> {
  fn clone(&self) -> Self {
    LazyPage {
      tx: self.tx,
      root: self.root.clone(),
      r: self.r,
    }
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> LazyPage<'tx, L> {
  pub fn new(
    root: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes, r: &'tx L,
  ) -> Self {
    let mut page = LazyPage {
      tx: Default::default(),
      root,
      r: None,
    };
    if page.page_header().get_overflow() > 0 {
      page.r = Some(r)
    };
    page
  }

  pub fn len(&self) -> usize {
    self.root_page().len() * (self.page_header().get_overflow() + 1) as usize
  }

  pub fn read_overflow_page(
    &self, overflow_index: u32,
  ) -> crate::Result<
    <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
    PageError,
  > {
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

impl<'tx, L: TxReadLazyPageIO<'tx>> Page for LazyPage<'tx, L> {
  #[inline]
  fn root_page(&self) -> &[u8] {
    self.root.as_ref()
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> GetKvRefSlice for LazyPage<'tx, L> {
  type RefKv<'a>
    = LazyRefSlice<'a, 'tx, L>
  where
    Self: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    let range = (0..self.len()).sub_range(range);
    LazyRefSlice::new(self, range)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> GetKvTxSlice<'tx> for LazyPage<'tx, L> {
  type TxKv = LazyTxSlice<'tx, L>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = (0..self.len()).sub_range(range);
    LazyTxSlice::new(self.clone(), range)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TxPageType<'tx> for LazyPage<'tx, L> {
  type TxPageBytes = SharedTxBytes<'tx>;
}

#[derive(Clone)]
pub struct LazyIter<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> {
  page: &'a LazyPage<'tx, L>,
  range: Range<usize>,
  next_overflow_index: u32,
  next_page: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  next_back_overflow_index: u32,
  next_back_page: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> LazyIter<'a, 'tx, L> {
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

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Iterator for LazyIter<'a, 'tx, L> {
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

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> DoubleEndedIterator for LazyIter<'a, 'tx, L> {
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

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> ExactSizeIterator for LazyIter<'a, 'tx, L> {
  #[inline]
  fn len(&self) -> usize {
    self.range.len()
  }
}
