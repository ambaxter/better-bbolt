use crate::common::buffer_pool::PoolBuffer;
use crate::io::TxSlot;
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::ops::{GetKvRefSlice, GetKvTxSlice, KvDataType, RefIntoCopiedIter, SubRange};
use std::cmp::Ordering;
use std::iter::Copied;
use std::ops::{Deref, Range, RangeBounds};
use std::slice;
use triomphe::{Arc, UniqueArc};

#[derive(Clone)]
pub struct SharedBytes {
  pub(crate) inner: Option<Arc<PoolBuffer>>,
}

impl Deref for SharedBytes {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl AsRef<[u8]> for SharedBytes {
  fn as_ref(&self) -> &[u8] {
    self
      .inner
      .as_ref()
      .expect("shared buffer is dropped")
      .slice
      .as_ref()
  }
}

impl Drop for SharedBytes {
  fn drop(&mut self) {
    let inner = self.inner.take().expect("shared buffer is dropped");
    if inner.is_unique() {
      let mut inner: UniqueArc<PoolBuffer> = inner.try_into().expect("shared buffer isn't unique?");
      if let Some(pool) = inner.header.take() {
        pool.push(inner);
      }
    }
  }
}

impl IOBytes for SharedBytes {}

#[derive(Clone)]
pub struct SharedTxBytes<'tx> {
  tx: TxSlot<'tx>,
  inner: SharedBytes,
}

impl<'tx> SharedTxBytes<'tx> {
  pub fn new(inner: SharedBytes) -> Self {
    Self {
      tx: Default::default(),
      inner,
    }
  }
}

impl<'tx> Deref for SharedTxBytes<'tx> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.inner.as_ref()
  }
}

impl<'tx> AsRef<[u8]> for SharedTxBytes<'tx> {
  fn as_ref(&self) -> &[u8] {
    self.inner.as_ref()
  }
}

impl<'tx> TxBytes<'tx> for SharedTxBytes<'tx> {}

impl<'tx> FromIOBytes<'tx, SharedBytes> for SharedTxBytes<'tx> {
  fn from_io(value: SharedBytes) -> Self {
    SharedTxBytes::new(value)
  }
}

impl<'tx> PartialOrd for SharedTxBytes<'tx> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }

  #[inline]
  fn lt(&self, other: &Self) -> bool {
    self.as_ref().lt(other.as_ref())
  }

  #[inline]
  fn le(&self, other: &Self) -> bool {
    self.as_ref().le(other.as_ref())
  }

  #[inline]
  fn gt(&self, other: &Self) -> bool {
    self.as_ref().gt(other.as_ref())
  }

  #[inline]
  fn ge(&self, other: &Self) -> bool {
    self.as_ref().ge(other.as_ref())
  }
}

impl<'tx> PartialEq for SharedTxBytes<'tx> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'tx> Eq for SharedTxBytes<'tx> {}

impl<'tx> Ord for SharedTxBytes<'tx> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SharedRefSlice<'a> {
  pub(crate) inner: &'a [u8],
}

impl<'a> AsRef<[u8]> for SharedRefSlice<'a> {
  fn as_ref(&self) -> &[u8] {
    self.inner
  }
}

#[derive(Clone)]
pub struct SharedTxSlice<'tx> {
  pub(crate) inner: SharedTxBytes<'tx>,
  pub(crate) range: Range<usize>,
}

impl<'tx> AsRef<[u8]> for SharedTxSlice<'tx> {
  fn as_ref(&self) -> &[u8] {
    &self.inner.inner.as_ref()[(
      self.range.start_bound().cloned(),
      self.range.end_bound().cloned(),
    )]
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

impl<'tx> GetKvRefSlice for SharedTxBytes<'tx> {
  type RefKv<'a>
    = SharedRefSlice<'a>
  where
    Self: 'a;

  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

impl<'tx> GetKvTxSlice<'tx> for SharedTxBytes<'tx> {
  type TxKv = SharedTxSlice<'tx>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = (0..self.len()).sub_range(range);
    SharedTxSlice {
      inner: self.clone(),
      range,
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

impl<'p> GetKvRefSlice for SharedRefSlice<'p> {
  type RefKv<'a>
    = SharedRefSlice<'a>
  where
    Self: 'a,
    'p: 'a;

  #[inline]
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
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'tx> PartialOrd for SharedTxSlice<'tx> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }

  #[inline]
  fn lt(&self, other: &Self) -> bool {
    self.as_ref().lt(other.as_ref())
  }

  #[inline]
  fn le(&self, other: &Self) -> bool {
    self.as_ref().le(other.as_ref())
  }

  #[inline]
  fn gt(&self, other: &Self) -> bool {
    self.as_ref().gt(other.as_ref())
  }

  #[inline]
  fn ge(&self, other: &Self) -> bool {
    self.as_ref().ge(other.as_ref())
  }
}

impl<'tx> Eq for SharedTxSlice<'tx> {}

impl<'tx> Ord for SharedTxSlice<'tx> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl<'tx> GetKvRefSlice for SharedTxSlice<'tx> {
  type RefKv<'a>
    = SharedRefSlice<'a>
  where
    Self: 'a,
    'tx: 'a;

  #[inline]
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
