use crate::common::errors::{OpsError, PageError};
use crate::io::pages::lazy::ops::{
  KvTryDataType, KvTryEq, KvTryOrd, LazyRefIntoTryBuf, RefIntoTryBuf, TryBuf, TryEq, TryGet,
  TryHash, TryPartialEq, TryPartialOrd,
};
use crate::io::pages::lazy::{
  LazyIter, LazyPage, try_partial_cmp_buf_lazy_buf, try_partial_cmp_lazy_buf_buf,
  try_partial_cmp_lazy_buf_lazy_buf, try_partial_eq_buf_lazy_buf, try_partial_eq_lazy_buf_buf,
  try_partial_eq_lazy_buf_lazy_buf,
};
use crate::io::pages::{
  GatKvRef, GetGatKvRefSlice, SubRange, TxPageType, TxReadLazyPageIO, TxReadPageIO,
};
use error_stack::ResultExt;
use std::cmp::Ordering;
use std::hash;
use std::ops::{Range, RangeBounds};

pub struct LazyRefSlice<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> {
  pub(crate) page: &'a LazyPage<'tx, L>,
  pub(crate) range: Range<usize>,
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> LazyRefSlice<'a, 'tx, L> {
  pub fn new(page: &'a LazyPage<'tx, L>, range: Range<usize>) -> Self {
    Self { page, range }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Clone for LazyRefSlice<'a, 'tx, L> {
  fn clone(&self) -> Self {
    LazyRefSlice {
      page: self.page,
      range: self.range.clone(),
    }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialEq for LazyRefSlice<'a, 'tx, L> {
  fn eq(&self, other: &Self) -> bool {
    TryPartialEq::try_eq(self, other).expect("partial_eq error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialEq<[u8]> for LazyRefSlice<'a, 'tx, L> {
  fn eq(&self, other: &[u8]) -> bool {
    TryPartialEq::try_eq(self, other).expect("partial_eq error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Eq for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialOrd for LazyRefSlice<'a, 'tx, L> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    TryPartialOrd::try_partial_cmp(self, other).expect("partial_ord error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialOrd<[u8]> for LazyRefSlice<'a, 'tx, L> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    TryPartialOrd::try_partial_cmp(self, other).expect("partial_ord error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Ord for LazyRefSlice<'a, 'tx, L> {
  fn cmp(&self, other: &Self) -> Ordering {
    TryPartialOrd::try_partial_cmp(self, other)
      .expect("partial_ord error")
      .expect("cannot be empty")
  }
}

impl<'a, 'p, 'tx: 'p, L: TxReadLazyPageIO<'tx>> GatKvRef<'a> for LazyRefSlice<'p, 'tx, L> {
  type KvRef = LazyRefSlice<'a, 'tx, L>;
}

impl<'p, 'tx: 'p, L: TxReadLazyPageIO<'tx>> GetGatKvRefSlice for LazyRefSlice<'p, 'tx, L> {
  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    LazyRefSlice {
      page: self.page,
      range: self.range.sub_range_bound(range),
    }
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> TryHash for LazyRefSlice<'p, 'tx, L> {
  type Error = OpsError;

  fn try_hash<H: hash::Hasher>(&self, state: &mut H) -> crate::Result<(), Self::Error> {
    todo!()
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> hash::Hash for LazyRefSlice<'p, 'tx, L> {
  fn hash<H: hash::Hasher>(&self, state: &mut H) {
    self.try_hash(state).expect("hashing error")
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> TryGet<u8> for LazyRefSlice<'p, 'tx, L> {
  type Error = OpsError;

  fn try_get(&self, index: usize) -> crate::Result<Option<u8>, Self::Error> {
    todo!()
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> RefIntoTryBuf for LazyRefSlice<'p, 'tx, L> {
  type TryBuf<'a>
    = LazyRefTryBuf<'a, 'tx, L>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error> {
    LazyRefTryBuf::new(self)
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> LazyRefIntoTryBuf for LazyRefSlice<'p, 'tx, L> {}

pub struct LazyRefTryBuf<'a, 'tx, L: TxReadLazyPageIO<'tx>> {
  slice: LazyRefSlice<'a, 'tx, L>,
  range: Range<usize>,
  page: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  overflow_index: u32,
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> LazyRefTryBuf<'a, 'tx, L> {
  pub fn new(slice: &LazyRefSlice<'a, 'tx, L>) -> crate::Result<Self, PageError> {
    let range = slice.range.clone();
    let overflow_index = (range.start / slice.page.root.as_ref().len()) as u32;
    let page_result = if overflow_index == 0 {
      Ok(slice.page.root.clone())
    } else {
      slice.page.read_overflow_page(overflow_index)
    };
    page_result.map(|page| LazyRefTryBuf {
      slice: (*slice).clone(),
      range,
      page,
      overflow_index,
    })
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryBuf for LazyRefTryBuf<'a, 'tx, L> {
  type Error = PageError;

  fn remaining(&self) -> usize {
    self.range.len()
  }

  fn chunk(&self) -> &[u8] {
    let page_size = self.slice.page.root.as_ref().len();
    let overflow_start = self.overflow_index as usize * page_size;
    assert!(overflow_start <= self.range.start);
    let page_len = self.page.as_ref().len();
    let page_start = self.range.start - overflow_start;
    let page_end = page_len.min(self.range.end - overflow_start);
    &self.page.as_ref()[page_start..page_end]
  }

  fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
    let overflow_index = (self.range.start / self.slice.page.root.as_ref().len()) as u32;
    if overflow_index != self.overflow_index {
      let page = self.slice.page.read_overflow_page(overflow_index)?;
      self.overflow_index = overflow_index;
      self.page = page;
    }
    self.range = self.range.sub_range_bound(cnt..);
    Ok(())
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryPartialEq for LazyRefSlice<'a, 'tx, L> {
  type Error = OpsError;

  fn try_eq(&self, other: &Self) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_lazy_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryPartialOrd for LazyRefSlice<'a, 'tx, L> {
  fn try_partial_cmp(&self, other: &Self) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_lazy_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialEq<T> for LazyRefSlice<'a, 'tx, L>
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &T) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialEq<LazyRefSlice<'a, 'tx, L>> for T
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &LazyRefSlice<'a, 'tx, L>) -> crate::Result<bool, Self::Error> {
    try_partial_eq_buf_lazy_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialOrd<T> for LazyRefSlice<'a, 'tx, L>
where
  T: AsRef<[u8]>,
{
  fn try_partial_cmp(&self, other: &T) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialOrd<LazyRefSlice<'a, 'tx, L>> for T
where
  T: AsRef<[u8]>,
{
  fn try_partial_cmp(
    &self, other: &LazyRefSlice<'a, 'tx, L>,
  ) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_buf_lazy_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryPartialEq<[u8]> for LazyRefSlice<'a, 'tx, L> {
  type Error = OpsError;

  fn try_eq(&self, other: &[u8]) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryPartialEq<LazyRefSlice<'a, 'tx, L>> for [u8] {
  type Error = OpsError;

  fn try_eq(&self, other: &LazyRefSlice<'a, 'tx, L>) -> crate::Result<bool, Self::Error> {
    try_partial_eq_buf_lazy_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryPartialOrd<[u8]> for LazyRefSlice<'a, 'tx, L> {
  fn try_partial_cmp(&self, other: &[u8]) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_buf(self, other)
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryEq for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> KvTryEq for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> KvTryOrd for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> KvTryDataType for LazyRefSlice<'a, 'tx, L> {}
