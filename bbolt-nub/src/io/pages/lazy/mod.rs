use crate::common::errors::{OpsError, PageError};
use crate::common::id::OverflowPageId;
use crate::io::TxSlot;
use crate::io::bytes::shared_bytes::SharedTxBytes;
use crate::io::ops::RefIntoBuf;
use crate::io::pages::lazy::ops::{LazyRefIntoTryBuf, TryBuf};
use crate::io::pages::lazy::ref_slice::LazyRefTryBuf;
use crate::io::pages::lazy::tx_slice::LazyTxSlice;
use crate::io::pages::{
  GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, SubRange, TxPageType, TxReadLazyPageIO,
  TxReadPageIO,
};
use bytes::Buf;
use error_stack::{FutureExt, ResultExt};
use ref_slice::LazyRefSlice;
use std::cmp::Ordering;
use std::ops::{Deref, Range, RangeBounds};
use std::{hash, sync};
use tracing::warn;

pub mod ops;
pub mod ref_slice;
pub mod tx_slice;

pub struct LazyPage<'tx, L: TxReadLazyPageIO<'tx>> {
  tx: TxSlot<'tx>,
  root: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  r: Option<sync::Arc<L>>,
}

unsafe impl<'tx, L: TxReadLazyPageIO<'tx>> Send for LazyPage<'tx, L> {}
unsafe impl<'tx, L: TxReadLazyPageIO<'tx>> Sync for LazyPage<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> Clone for LazyPage<'tx, L> {
  fn clone(&self) -> Self {
    LazyPage {
      tx: self.tx,
      root: self.root.clone(),
      r: self.r.clone(),
    }
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> LazyPage<'tx, L> {
  pub fn new(
    root: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes, r: &sync::Arc<L>,
  ) -> Self {
    let mut page = LazyPage {
      tx: Default::default(),
      root,
      r: None,
    };
    if page.page_header().get_overflow() > 0 {
      page.r = Some(r.clone());
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
      let r = self.r.as_ref().unwrap();
      match page_id {
        OverflowPageId::Freelist(page_id) => r.read_freelist_overflow(page_id, overflow_index),
        OverflowPageId::Node(page_id) => r.read_node_overflow(page_id, overflow_index),
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

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> GatKvRef<'a> for LazyPage<'tx, L> {
  type KvRef = LazyRefSlice<'a, 'tx, L>;
}

impl<'tx, L: TxReadLazyPageIO<'tx>> GetGatKvRefSlice for LazyPage<'tx, L> {
  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    let range = (0..self.len()).sub_range_bound(range);
    LazyRefSlice::new(self, range)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> GetKvTxSlice<'tx> for LazyPage<'tx, L> {
  type KvTx = LazyTxSlice<'tx, L>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    let range = (0..self.len()).sub_range_bound(range);
    LazyTxSlice::new(self.clone(), range)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TxPageType<'tx> for LazyPage<'tx, L> {
  type TxPageBytes = SharedTxBytes<'tx>;
}

#[derive(Clone)]
pub struct LazyTryIter<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> {
  page: &'a LazyPage<'tx, L>,
  range: Range<usize>,
  next_overflow_index: u32,
  next_page: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  next_back_overflow_index: u32,
  next_back_page: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> LazyTryIter<'a, 'tx, L> {
  pub fn new<R: RangeBounds<usize>>(
    page: &'a LazyPage<'tx, L>, range: R,
  ) -> crate::Result<LazyTryIter<'a, 'tx, L>, PageError> {
    let page_size = page.root_page().len();
    let overflow_count = page.page_header().get_overflow();
    let range = (0..page.len()).sub_range_bound(range);
    let next_overflow_index = (range.start / page_size) as u32;
    let next_page = page.read_overflow_page(next_overflow_index)?;
    let next_back_overflow_index = (range.end / page_size) as u32;
    let next_back_page = page.read_overflow_page(next_back_overflow_index)?;

    Ok(LazyTryIter {
      page,
      range,
      next_overflow_index,
      next_page,
      next_back_overflow_index,
      next_back_page,
    })
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Iterator for LazyTryIter<'a, 'tx, L> {
  type Item = crate::Result<u8, PageError>;
  fn next(&mut self) -> Option<Self::Item> {
    let index = self.range.next()?;
    let page_size = self.page.root_page().len();
    let next_overflow_index = (index / page_size) as u32;
    if next_overflow_index != self.next_overflow_index {
      let load_page = self.page.read_overflow_page(next_overflow_index);
      self.next_page = match load_page {
        Ok(page) => page,
        Err(err) => return Some(Err(err)),
      };
      self.next_overflow_index = next_overflow_index;
    }
    let page_index = index % page_size;
    self
      .next_page
      .as_ref()
      .get(page_index)
      .copied()
      .map(|b| Ok(b))
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> DoubleEndedIterator for LazyTryIter<'a, 'tx, L> {
  fn next_back(&mut self) -> Option<Self::Item> {
    let index = self.range.next_back()?;
    let page_size = self.page.root_page().len();
    let next_back_overflow_index = (index / page_size) as u32;
    if next_back_overflow_index != self.next_back_overflow_index {
      let load_page = self.page.read_overflow_page(next_back_overflow_index);
      self.next_back_page = match load_page {
        Ok(page) => page,
        Err(err) => return Some(Err(err)),
      };
      self.next_back_overflow_index = next_back_overflow_index;
    }
    let page_index = index % page_size;
    self
      .next_back_page
      .as_ref()
      .get(page_index)
      .copied()
      .map(|b| Ok(b))
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> ExactSizeIterator for LazyTryIter<'a, 'tx, L> {
  #[inline]
  fn len(&self) -> usize {
    self.range.len()
  }
}

// TODO: Change this to TryCopyIter by default and then panicable
#[derive(Clone)]
pub struct LazyIter<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> {
  iter: LazyTryIter<'a, 'tx, L>,
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> LazyIter<'a, 'tx, L> {
  pub fn new<R: RangeBounds<usize>>(page: &'a LazyPage<'tx, L>, range: R) -> LazyIter<'a, 'tx, L> {
    let iter = LazyTryIter::new(page, range).expect("lazy iter");
    LazyIter { iter }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Iterator for LazyIter<'a, 'tx, L> {
  type Item = u8;
  fn next(&mut self) -> Option<Self::Item> {
    match self.iter.next() {
      Some(r) => Some(r.expect("lazy iter")),
      None => None,
    }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> DoubleEndedIterator for LazyIter<'a, 'tx, L> {
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.iter.next_back() {
      Some(r) => Some(r.expect("lazy iter")),
      None => None,
    }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> ExactSizeIterator for LazyIter<'a, 'tx, L> {
  #[inline]
  fn len(&self) -> usize {
    self.iter.len()
  }
}

pub fn try_partial_eq_lazy_buf_lazy_buf<T, U>(s: &T, mut o: &U) -> crate::Result<bool, OpsError>
where
  T: LazyRefIntoTryBuf,
  U: LazyRefIntoTryBuf,
{
  let mut s_buf = s
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  let mut o_buf = o
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  if s_buf.remaining() != o_buf.remaining() {
    return Ok(false);
  }
  while s_buf.remaining() > 0 {
    let s_chunk = s_buf.chunk();
    let o_chunk = o_buf.chunk();
    let cmp_len = s_chunk.len().min(o_chunk.len());
    //TODO: What do we do here?
    assert_ne!(0, cmp_len);
    let s_cmp = &s_chunk[..cmp_len];
    let o_cmp = &o_chunk[..cmp_len];
    if s_cmp != o_cmp {
      return Ok(false);
    }
    s_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialEq)?;
    o_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialEq)?;
  }
  Ok(true)
}

pub fn try_partial_eq_lazy_buf_buf<T, U: ?Sized>(s: &T, mut o: &U) -> crate::Result<bool, OpsError>
where
  T: LazyRefIntoTryBuf,
  U: RefIntoBuf,
{
  let mut s_buf = s
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  let mut o_buf = o.ref_into_buf();
  if s_buf.remaining() != o_buf.remaining() {
    return Ok(false);
  }
  while s_buf.remaining() > 0 {
    let s_chunk = s_buf.chunk();
    let o_chunk = o_buf.chunk();
    let cmp_len = s_chunk.len().min(o_chunk.len());
    //TODO: What do we do here?
    assert_ne!(0, cmp_len);
    let s_cmp = &s_chunk[..cmp_len];
    let o_cmp = &o_chunk[..cmp_len];
    if s_cmp != o_cmp {
      return Ok(false);
    }
    s_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialEq)?;
    o_buf.advance(cmp_len);
  }
  Ok(true)
}

pub fn try_partial_eq_buf_lazy_buf<T: ?Sized, U>(s: &T, o: &U) -> crate::Result<bool, OpsError>
where
  T: RefIntoBuf,
  U: LazyRefIntoTryBuf,
{
  let mut s_buf = s.ref_into_buf();
  let mut o_buf = o
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  if s_buf.remaining() != o_buf.remaining() {
    return Ok(false);
  }
  while s_buf.remaining() > 0 {
    let s_chunk = s_buf.chunk();
    let o_chunk = o_buf.chunk();
    let cmp_len = s_chunk.len().min(o_chunk.len());
    //TODO: What do we do here?
    assert_ne!(0, cmp_len);
    let s_cmp = &s_chunk[..cmp_len];
    let o_cmp = &o_chunk[..cmp_len];
    if s_cmp != o_cmp {
      return Ok(false);
    }
    s_buf.advance(cmp_len);
    o_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialEq)?;
  }
  Ok(true)
}

pub fn try_partial_cmp_lazy_buf_lazy_buf<T, U>(
  s: &T, o: &U,
) -> crate::Result<Option<Ordering>, OpsError>
where
  T: LazyRefIntoTryBuf,
  U: LazyRefIntoTryBuf,
{
  let mut s_buf = s
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  let mut o_buf = o
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  while s_buf.remaining() > 0 && o_buf.remaining() > 0 {
    let s_chunk = s_buf.chunk();
    let o_chunk = o_buf.chunk();
    let cmp_len = s_chunk.len().min(o_chunk.len());
    assert_ne!(0, cmp_len);
    let s_cmp = &s_chunk[..cmp_len];
    let o_cmp = &o_chunk[..cmp_len];
    let cmp = s_cmp.cmp(o_cmp);
    if cmp != Ordering::Equal {
      return Ok(Some(cmp));
    }
    s_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialOrd)?;
    o_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialOrd)?;
  }
  Ok(s_buf.remaining().partial_cmp(&o_buf.remaining()))
}

pub fn try_partial_cmp_lazy_buf_buf<T, U: ?Sized>(
  s: &T, o: &U,
) -> crate::Result<Option<Ordering>, OpsError>
where
  T: LazyRefIntoTryBuf,
  U: RefIntoBuf,
{
  let mut s_buf = s
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;
  let mut o_buf = o.ref_into_buf();
  while s_buf.remaining() > 0 && o_buf.remaining() > 0 {
    let s_chunk = s_buf.chunk();
    let o_chunk = o_buf.chunk();
    let cmp_len = s_chunk.len().min(o_chunk.len());
    assert_ne!(0, cmp_len);
    let s_cmp = &s_chunk[..cmp_len];
    let o_cmp = &o_chunk[..cmp_len];
    let cmp = s_cmp.cmp(o_cmp);
    if cmp != Ordering::Equal {
      return Ok(Some(cmp));
    }
    s_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialOrd)?;
    o_buf.advance(cmp_len);
  }
  Ok(s_buf.remaining().partial_cmp(&o_buf.remaining()))
}

pub fn try_partial_cmp_buf_lazy_buf<T: ?Sized, U>(
  s: &T, o: &U,
) -> crate::Result<Option<Ordering>, OpsError>
where
  T: RefIntoBuf,
  U: LazyRefIntoTryBuf,
{
  let mut s_buf = s.ref_into_buf();
  let mut o_buf = o
    .ref_into_try_buf()
    .change_context(OpsError::TryPartialEq)?;

  while s_buf.remaining() > 0 && o_buf.remaining() > 0 {
    let s_chunk = s_buf.chunk();
    let o_chunk = o_buf.chunk();
    let cmp_len = s_chunk.len().min(o_chunk.len());
    assert_ne!(0, cmp_len);
    let s_cmp = &s_chunk[..cmp_len];
    let o_cmp = &o_chunk[..cmp_len];
    let cmp = s_cmp.cmp(o_cmp);
    if cmp != Ordering::Equal {
      return Ok(Some(cmp));
    }
    s_buf.advance(cmp_len);
    o_buf
      .try_advance(cmp_len)
      .change_context(OpsError::TryPartialOrd)?;
  }
  Ok(s_buf.remaining().partial_cmp(&o_buf.remaining()))
}
