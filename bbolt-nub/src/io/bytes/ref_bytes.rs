use crate::io::TxSlot;
use crate::io::bytes::shared_bytes::{SharedRefSlice, SharedTxSlice};
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::ops::{
  GetKvRefSlice, GetKvTxSlice, KvDataType, KvEq, KvOrd, RefIntoCopiedIter, SubRange,
  TryBuf, TryGet,
};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::iter::Copied;
use std::ops::{Deref, Range, RangeBounds};
use std::ptr::slice_from_raw_parts;
use std::{io, slice};
use crate::common::errors::OpsError;

#[derive(Debug, Clone)]
pub struct RefBytes {
  ptr: *const u8,
  len: usize,
}

impl RefBytes {
  pub(crate) fn from_ref(bytes: &[u8]) -> Self {
    Self {
      ptr: bytes.as_ptr(),
      len: bytes.len(),
    }
  }
}

impl AsRef<[u8]> for RefBytes {
  fn as_ref(&self) -> &[u8] {
    unsafe { slice::from_raw_parts(self.ptr, self.len) }
  }
}

impl Deref for RefBytes {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl IOBytes for RefBytes {}

#[derive(Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct RefTxBytes<'tx> {
  bytes: &'tx [u8],
}

impl<'tx> RefTxBytes<'tx> {
  pub fn new(bytes: &'tx [u8]) -> Self {
    Self { bytes }
  }

  pub fn as_tx_bytes(&self) -> &'tx [u8] {
    self.bytes
  }
}

impl<'tx> AsRef<[u8]> for RefTxBytes<'tx> {
  fn as_ref(&self) -> &[u8] {
    self.bytes
  }
}

impl<'tx> Deref for RefTxBytes<'tx> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.bytes
  }
}

impl<'tx> TxBytes<'tx> for RefTxBytes<'tx> {}

impl<'tx> FromIOBytes<'tx, RefBytes> for RefTxBytes<'tx> {
  fn from_io(value: RefBytes) -> RefTxBytes<'tx> {
    unsafe { RefTxBytes::new(slice::from_raw_parts(value.ptr, value.len)) }
  }
}

impl<'tx> RefIntoCopiedIter for RefTxBytes<'tx> {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.as_ref().iter().copied()
  }
}

impl<'tx> PartialEq<[u8]> for RefTxBytes<'tx> {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<'tx> PartialOrd<[u8]> for RefTxBytes<'tx> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
  }
}

impl<'tx> GetKvRefSlice for RefTxBytes<'tx> {
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

#[derive(Debug, Clone)]
pub struct RefTxSlice<'tx> {
  bytes: &'tx [u8],
  range: Range<usize>,
}

impl<'tx> AsRef<[u8]> for RefTxSlice<'tx> {
  fn as_ref(&self) -> &[u8] {
    &self.bytes[self.range.clone()]
  }
}

impl<'tx> Hash for RefTxSlice<'tx> {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state);
  }
}

impl<'tx> PartialEq for RefTxSlice<'tx> {
  #[inline]
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'tx> Eq for RefTxSlice<'tx> {}

impl<'tx> PartialOrd for RefTxSlice<'tx> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }
}

impl<'tx> Ord for RefTxSlice<'tx> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl<'tx> PartialEq<[u8]> for RefTxSlice<'tx> {
  #[inline]
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<'tx> PartialOrd<[u8]> for RefTxSlice<'tx> {
  #[inline]
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
  }
}

impl<'tx> GetKvTxSlice<'tx> for RefTxBytes<'tx> {
  type TxKv = RefTxSlice<'tx>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = (0..self.bytes.len()).sub_range(range);
    RefTxSlice {
      bytes: self.bytes,
      range,
    }
  }
}

impl<'tx> KvEq for RefTxSlice<'tx> {}
impl<'tx> KvOrd for RefTxSlice<'tx> {}

impl<'tx> RefIntoCopiedIter for RefTxSlice<'tx> {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.as_ref().iter().copied()
  }
}

impl<'tx> KvDataType for RefTxSlice<'tx> {}

impl<'tx> GetKvRefSlice for RefTxSlice<'tx> {
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

impl<'tx> GetKvTxSlice<'tx> for RefTxSlice<'tx> {
  type TxKv = Self;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    let range = self.range.sub_range(range);
    RefTxSlice {
      bytes: self.bytes,
      range,
    }
  }
}

pub struct RefTryBuf<'a> {
  buf: &'a [u8],
  range: Range<usize>,
}

impl<'a> RefTryBuf<'a> {
  pub fn new(buf: &'a [u8]) -> Self {
    Self {
      buf,
      range: 0..buf.len(),
    }
  }
}

impl<'a> TryBuf for RefTryBuf<'a> {
  type Error = OpsError;

  fn remaining(&self) -> usize {
    self.range.len()
  }

  fn chunk(&self) -> &[u8] {
    &self.buf[self.range.clone()]
  }

  fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
    self.range = self.range.sub_range(cnt..);
    Ok(())
  }
}

impl<'tx> KvEq for RefTxBytes<'tx> {}
impl<'tx> KvOrd for RefTxBytes<'tx> {}

impl<'tx> KvDataType for RefTxBytes<'tx> {}

impl RefIntoCopiedIter for [u8] {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;
  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.iter().copied()
  }
}
