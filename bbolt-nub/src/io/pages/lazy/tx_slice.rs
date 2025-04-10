use crate::common::errors::OpsError;
use crate::io::ops::{
  GetKvRefSlice, GetKvTxSlice, KvDataType, KvEq, KvOrd, KvTryEq, KvTryOrd, LazyRefIntoTryBuf,
  RefIntoBuf, RefIntoCopiedIter, RefIntoTryBuf, SubRange, TryBuf, TryGet, TryHash, TryPartialEq,
  TryPartialOrd,
};
use crate::io::pages::TxReadLazyPageIO;
use crate::io::pages::lazy::ref_slice::{LazyRefSlice, LazyRefTryBuf};
use crate::io::pages::lazy::{try_partial_cmp_buf_lazy_buf, try_partial_cmp_lazy_buf_buf, try_partial_cmp_lazy_buf_lazy_buf, try_partial_eq_buf_lazy_buf, try_partial_eq_lazy_buf_buf, try_partial_eq_lazy_buf_lazy_buf, LazyIter, LazyPage};
use error_stack::ResultExt;
use std::cmp::Ordering;
use std::hash;
use std::ops::{Range, RangeBounds};

#[derive(Clone)]
pub struct LazyTxSlice<'tx, L: TxReadLazyPageIO<'tx>> {
  page: LazyPage<'tx, L>,
  range: Range<usize>,
}

impl<'tx, L: TxReadLazyPageIO<'tx>> LazyTxSlice<'tx, L> {
  pub fn new(page: LazyPage<'tx, L>, range: Range<usize>) -> Self {
    Self { page, range }
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryHash for LazyTxSlice<'tx, L> {
  type Error = OpsError;

  fn try_hash<H: hash::Hasher>(&self, state: &mut H) -> Result<(), Self::Error> {
    todo!()
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> hash::Hash for LazyTxSlice<'tx, L> {
  fn hash<H: hash::Hasher>(&self, state: &mut H) {
    self.try_hash(state).expect("hashing error")
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryGet<u8> for LazyTxSlice<'tx, L> {
  type Error = OpsError;

  fn try_get(&self, index: usize) -> crate::Result<Option<u8>, Self::Error> {
    todo!()
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> RefIntoTryBuf for LazyTxSlice<'tx, L> {
  type TryBuf<'a>
    = LazyRefTryBuf<'a, 'tx, L>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error> {
    todo!()
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> LazyRefIntoTryBuf for LazyTxSlice<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> PartialEq for LazyTxSlice<'tx, L> {
  fn eq(&self, other: &Self) -> bool {
    TryPartialEq::try_eq(self, other).expect("partial_eq error")
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> PartialEq<[u8]> for LazyTxSlice<'tx, L> {
  fn eq(&self, other: &[u8]) -> bool {
    TryPartialEq::try_eq(self, &other).expect("partial_eq error")
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> Eq for LazyTxSlice<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> PartialOrd for LazyTxSlice<'tx, L> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    TryPartialOrd::try_partial_cmp(self, other).expect("partialord failure")
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> PartialOrd<[u8]> for LazyTxSlice<'tx, L> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    TryPartialOrd::try_partial_cmp(self, other).expect("partialord failure")
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> Ord for LazyTxSlice<'tx, L> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.partial_cmp(other).expect("ord failure")
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> RefIntoCopiedIter for LazyTxSlice<'tx, L> {
  type Iter<'a>
    = LazyIter<'a, 'tx, L>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    LazyIter::new(&self.page, self.range.clone())
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> GetKvRefSlice for LazyTxSlice<'tx, L> {
  type RefKv<'a>
    = LazyRefSlice<'a, 'tx, L>
  where
    Self: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    let range = self.range.sub_range(range);
    LazyRefSlice::new(&self.page, range)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> GetKvTxSlice<'tx> for LazyTxSlice<'tx, L> {
  type TxKv = LazyTxSlice<'tx, L>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = self.range.sub_range(range);
    LazyTxSlice {
      page: self.page.clone(),
      range,
    }
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryPartialEq for LazyTxSlice<'tx, L> {
  type Error = OpsError;

  fn try_eq(&self, other: &Self) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_lazy_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryPartialOrd for LazyTxSlice<'tx, L> {
  fn try_partial_cmp(&self, other: &Self) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_lazy_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialEq<T> for LazyTxSlice<'tx, L>
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &T) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_buf(self, other.as_ref())
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialEq<LazyTxSlice<'tx, L>> for T
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &LazyTxSlice<'tx, L>) -> crate::Result<bool, Self::Error> {
    try_partial_eq_buf_lazy_buf(self.as_ref(), other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialOrd<T> for LazyTxSlice<'tx, L>
where
  T: AsRef<[u8]>,
{
  fn try_partial_cmp(&self, other: &T) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_buf(self, other.as_ref())
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>, T> TryPartialOrd<LazyTxSlice<'tx, L>> for T
where
  T: AsRef<[u8]>,
{
  fn try_partial_cmp(
    &self, other: &LazyTxSlice<'tx, L>,
  ) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_buf_lazy_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryPartialEq<[u8]> for LazyTxSlice<'tx, L> {
  type Error = OpsError;

  fn try_eq(&self, other: &[u8]) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryPartialEq<LazyTxSlice<'tx, L>> for [u8] {
  type Error = OpsError;

  fn try_eq(&self, other: &LazyTxSlice<'tx, L>) -> crate::Result<bool, Self::Error> {
    try_partial_eq_buf_lazy_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryPartialOrd<[u8]> for LazyTxSlice<'tx, L> {
  fn try_partial_cmp(&self, other: &[u8]) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> TryPartialOrd<LazyTxSlice<'tx, L>> for [u8] {
  fn try_partial_cmp(
    &self, other: &LazyTxSlice<'tx, L>,
  ) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_buf_lazy_buf(self, other)
  }
}

impl<'tx, L: TxReadLazyPageIO<'tx>> KvTryEq for LazyTxSlice<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> KvEq for LazyTxSlice<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> KvTryOrd for LazyTxSlice<'tx, L> {}

impl<'tx, L: TxReadLazyPageIO<'tx>> KvOrd for LazyTxSlice<'tx, L> {}
impl<'tx, L: TxReadLazyPageIO<'tx>> KvDataType for LazyTxSlice<'tx, L> {}
