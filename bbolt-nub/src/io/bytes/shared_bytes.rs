use crate::common::buffer_pool::PoolBuffer;
use crate::common::errors::OpsError;
use crate::io::TxSlot;
use crate::io::bytes::ref_bytes::RefTryBuf;
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::pages::direct::ops::{DirectGet, KvDataType, KvEq, KvOrd};
use crate::io::pages::lazy::ops::{KvTryEq, KvTryOrd, TryEq};
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice, SubRange};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::iter::Copied;
use std::mem::ManuallyDrop;
use std::ops::{Deref, Range, RangeBounds};
use std::{io, slice};

#[derive(Clone)]
pub struct SharedBytes {
  pub(crate) inner: ManuallyDrop<triomphe::Arc<PoolBuffer>>,
}

impl Deref for SharedBytes {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl AsRef<[u8]> for SharedBytes {
  fn as_ref(&self) -> &[u8] {
    &self.inner.slice
  }
}

impl Drop for SharedBytes {
  fn drop(&mut self) {
    let inner = unsafe { ManuallyDrop::take(&mut self.inner) };
    if let Some(mut unique) = triomphe::Arc::try_unique(inner).ok() {
      if let Some(pool) = unique.header.take() {
        pool.push(unique);
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
  #[inline]
  fn deref(&self) -> &Self::Target {
    self.inner.as_ref()
  }
}

impl<'tx> AsRef<[u8]> for SharedTxBytes<'tx> {
  #[inline]
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

impl<'tx> TryEq for SharedTxBytes<'tx> {}

impl<'tx> DirectGet<u8> for SharedTxBytes<'tx> {
  fn direct_get(&self, index: usize) -> Option<u8> {
    self.as_ref().get(index).copied()
  }
}

impl<'tx> KvTryEq for SharedTxBytes<'tx> {}
impl<'tx> KvEq for SharedTxBytes<'tx> {}
impl<'tx> KvTryOrd for SharedTxBytes<'tx> {}
impl<'tx> KvOrd for SharedTxBytes<'tx> {}
impl<'tx> KvDataType for SharedTxBytes<'tx> {}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct SharedRefSlice<'a> {
  pub(crate) inner: &'a [u8],
}

impl<'a> Deref for SharedRefSlice<'a> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.inner
  }
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

impl<'tx> Deref for SharedTxSlice<'tx> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
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
impl<'a, 'tx> GatKvRef<'a> for SharedTxBytes<'tx> {
  type KvRef = SharedRefSlice<'a>;
}

impl<'tx> GetGatKvRefSlice for SharedTxBytes<'tx> {
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

impl<'tx> GetKvTxSlice<'tx> for SharedTxBytes<'tx> {
  type KvTx = SharedTxSlice<'tx>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    let range = (0..self.len()).sub_range_bound(range);
    SharedTxSlice {
      inner: self.clone(),
      range,
    }
  }
}

// SharedRefSlice<'a> //

impl<'a, 'p> GatKvRef<'a> for SharedRefSlice<'p> {
  type KvRef = SharedRefSlice<'a>;
}

impl<'p> GetGatKvRefSlice for SharedRefSlice<'p> {
  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
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

impl<'tx> TryEq for SharedRefSlice<'tx> {}

impl<'tx> DirectGet<u8> for SharedRefSlice<'tx> {
  fn direct_get(&self, index: usize) -> Option<u8> {
    self.as_ref().get(index).copied()
  }
}

impl<'tx> KvTryEq for SharedRefSlice<'tx> {}
impl<'tx> KvEq for SharedRefSlice<'tx> {}
impl<'tx> KvTryOrd for SharedRefSlice<'tx> {}
impl<'tx> KvOrd for SharedRefSlice<'tx> {}
impl<'tx> KvDataType for SharedRefSlice<'tx> {}

// SharedTxSlice<'tx> //

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

impl<'a, 'tx> GatKvRef<'a> for SharedTxSlice<'tx> {
  type KvRef = SharedRefSlice<'a>;
}

impl<'tx> GetGatKvRefSlice for SharedTxSlice<'tx> {
  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

impl<'tx> GetKvTxSlice<'tx> for SharedTxSlice<'tx> {
  type KvTx = Self;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    SharedTxSlice {
      inner: self.inner.clone(),
      range: self.range.sub_range_bound(range),
    }
  }
}

impl<'tx> Hash for SharedTxSlice<'tx> {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl<'tx> TryEq for SharedTxSlice<'tx> {}

impl<'tx> DirectGet<u8> for SharedTxSlice<'tx> {
  fn direct_get(&self, index: usize) -> Option<u8> {
    self.as_ref().get(index).copied()
  }
}

impl<'tx> KvTryEq for SharedTxSlice<'tx> {}
impl<'tx> KvEq for SharedTxSlice<'tx> {}
impl<'tx> KvTryOrd for SharedTxSlice<'tx> {}
impl<'tx> KvOrd for SharedTxSlice<'tx> {}
impl<'tx> KvDataType for SharedTxSlice<'tx> {}
