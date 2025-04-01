use crate::common::buffer_pool::PoolBuffer;
use crate::io::TxSlot;
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::pages::KvDataType;
use std::cmp::Ordering;
use std::ops::{Deref, Range, RangeBounds};
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
