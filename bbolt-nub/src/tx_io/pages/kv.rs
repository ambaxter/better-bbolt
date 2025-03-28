use crate::tx_io::bytes::shared_bytes::{SharedRefSlice, SharedTxBytes, SharedTxSlice};
use crate::tx_io::pages::{GetKvRefSlice, GetKvTxSlice, KvDataType, RefIntoCopiedIter, SubRange};
use std::cmp::Ordering;
use std::iter::Copied;
use std::ops::RangeBounds;
use std::slice;

// &'a [u8] //

impl<'p> RefIntoCopiedIter for &'p [u8] {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a,
    'p: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.iter().copied()
  }
}

impl<'tx> KvDataType for &'tx [u8] {
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
}

impl<'p> GetKvRefSlice for &'p [u8] {
  type RefKv<'a>
    = &'a [u8]
  where
    Self: 'a,
    'p: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

impl<'tx> GetKvTxSlice<'tx> for &'tx [u8] {
  type TxKv = &'tx [u8];

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

// Shared Tx Bytes //

impl<'tx> RefIntoCopiedIter for SharedTxBytes<'tx> {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.iter().copied()
  }
}

impl<'tx> KvDataType for SharedTxBytes<'tx> {
  #[inline]
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }

  #[inline]
  fn lt(&self, other: &[u8]) -> bool {
    self.as_ref().lt(other)
  }

  #[inline]
  fn le(&self, other: &[u8]) -> bool {
    self.as_ref().le(other)
  }

  #[inline]
  fn gt(&self, other: &[u8]) -> bool {
    self.as_ref().gt(other)
  }

  #[inline]
  fn ge(&self, other: &[u8]) -> bool {
    self.as_ref().ge(other)
  }
}

impl<'tx> GetKvRefSlice for SharedTxBytes<'tx> {
  type RefKv<'a>
    = SharedRefSlice<'a>
  where
    Self: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

// SharedRefSlice<'a> //

impl<'p> RefIntoCopiedIter for SharedRefSlice<'p> {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a,
    'p: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.inner.iter().copied()
  }
}

impl<'a> KvDataType for SharedRefSlice<'a> {
  #[inline]
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.inner.eq(other)
  }

  #[inline]
  fn lt(&self, other: &[u8]) -> bool {
    self.inner.lt(other)
  }

  #[inline]
  fn le(&self, other: &[u8]) -> bool {
    self.inner.le(other)
  }

  #[inline]
  fn gt(&self, other: &[u8]) -> bool {
    self.inner.gt(other)
  }

  #[inline]
  fn ge(&self, other: &[u8]) -> bool {
    self.inner.ge(other)
  }
}

impl<'p> GetKvRefSlice for SharedRefSlice<'p> {
  type RefKv<'a>
    = SharedRefSlice<'a>
  where
    Self: 'a,
    'p: 'a;
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

// SharedTxSlice<'tx> //

impl<'tx> RefIntoCopiedIter for SharedTxSlice<'tx> {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.as_ref().iter().copied()
  }
}

impl<'tx> PartialEq<Self> for SharedTxSlice<'tx> {
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'tx> PartialOrd for SharedTxSlice<'tx> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }

  fn lt(&self, other: &Self) -> bool {
    self.as_ref().lt(other.as_ref())
  }

  fn le(&self, other: &Self) -> bool {
    self.as_ref().le(other.as_ref())
  }

  fn gt(&self, other: &Self) -> bool {
    self.as_ref().gt(other.as_ref())
  }

  fn ge(&self, other: &Self) -> bool {
    self.as_ref().ge(other.as_ref())
  }
}

impl<'tx> Eq for SharedTxSlice<'tx> {}

impl<'tx> Ord for SharedTxSlice<'tx> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl<'tx> KvDataType for SharedTxSlice<'tx> {
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }

  fn lt(&self, other: &[u8]) -> bool {
    self.as_ref().lt(other)
  }

  fn le(&self, other: &[u8]) -> bool {
    self.as_ref().le(other)
  }

  fn gt(&self, other: &[u8]) -> bool {
    self.as_ref().gt(other)
  }

  fn ge(&self, other: &[u8]) -> bool {
    self.as_ref().ge(other)
  }
}

impl<'tx> GetKvRefSlice for SharedTxSlice<'tx> {
  type RefKv<'a>
    = SharedRefSlice<'a>
  where
    Self: 'a,
    'tx: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

impl<'tx> GetKvTxSlice<'tx> for SharedTxSlice<'tx> {
  type TxKv = Self;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    SharedTxSlice {
      inner: self.inner.clone(),
      range: self.range.sub_range(range),
    }
  }
}
