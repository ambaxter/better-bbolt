use crate::common::buffer_pool::PoolBuffer;
use crate::io::TxSlot;
use crate::io::bytes::ref_bytes::RefTryBuf;
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::ops::{
  GetKvRefSlice, GetKvTxSlice, KvDataType, KvEq, KvOrd, RefIntoCopiedIter, RefIntoTryBuf, SubRange,
  TryGet,
};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::iter::Copied;
use std::ops::{Deref, Range, RangeBounds};
use std::{io, slice};
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
}

impl<'tx> PartialEq for SharedTxBytes<'tx> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'tx> Eq for SharedTxBytes<'tx> {}

impl<'tx> PartialEq<[u8]> for SharedTxBytes<'tx> {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<'tx> PartialOrd<[u8]> for SharedTxBytes<'tx> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
  }
}

impl<'tx> Ord for SharedTxBytes<'tx> {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl<'tx> Hash for SharedTxBytes<'tx> {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state)
  }
}

impl<'tx> RefIntoTryBuf for SharedTxBytes<'tx> {
  type Error = io::Error;
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
    Ok(RefTryBuf::new(self))
  }
}

impl<'tx> TryGet<u8> for SharedTxBytes<'tx> {
  type Error = io::Error;

  fn try_get(&self, index: usize) -> Result<Option<u8>, Self::Error> {
    Ok(self.as_ref().get(index).copied())
  }
}

impl<'tx> KvEq for SharedTxBytes<'tx> {}
impl<'tx> KvOrd for SharedTxBytes<'tx> {}
impl<'tx> KvDataType for SharedTxBytes<'tx> {}

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

impl<'tx> RefIntoTryBuf for SharedRefSlice<'tx> {
  type Error = io::Error;
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
    Ok(RefTryBuf::new(self.as_ref()))
  }
}

impl<'tx> TryGet<u8> for SharedRefSlice<'tx> {
  type Error = io::Error;

  fn try_get(&self, index: usize) -> Result<Option<u8>, Self::Error> {
    Ok(self.as_ref().get(index).copied())
  }
}

impl<'tx> Hash for SharedRefSlice<'tx> {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl<'tx> PartialEq<[u8]> for SharedRefSlice<'tx> {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<'tx> PartialOrd<[u8]> for SharedRefSlice<'tx> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
  }
}

impl<'tx> KvEq for SharedRefSlice<'tx> {}
impl<'tx> KvOrd for SharedRefSlice<'tx> {}
impl<'tx> KvDataType for SharedRefSlice<'tx> {}

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

impl<'tx> PartialEq<[u8]> for SharedTxSlice<'tx> {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<'tx> PartialOrd for SharedTxSlice<'tx> {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }
}

impl<'tx> PartialOrd<[u8]> for SharedTxSlice<'tx> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
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

impl<'tx> Hash for SharedTxSlice<'tx> {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl<'tx> RefIntoTryBuf for SharedTxSlice<'tx> {
  type Error = io::Error;
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
    Ok(RefTryBuf::new(self.as_ref()))
  }
}

impl<'tx> TryGet<u8> for SharedTxSlice<'tx> {
  type Error = io::Error;

  fn try_get(&self, index: usize) -> Result<Option<u8>, Self::Error> {
    Ok(self.as_ref().get(index).copied())
  }
}

impl<'tx> KvEq for SharedTxSlice<'tx> {}
impl<'tx> KvOrd for SharedTxSlice<'tx> {}
impl<'tx> KvDataType for SharedTxSlice<'tx> {}
