use crate::common::errors::OpsError;
use crate::io::TxSlot;
use crate::io::bytes::shared_bytes::{SharedRefSlice, SharedTxSlice};
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::ops::{Buf, RefIntoCopiedIter};
use crate::io::pages::direct::ops::{Get, KvDataType, KvEq, KvOrd};
use crate::io::pages::lazy::ops::{KvTryEq, KvTryOrd, TryBuf, TryEq, TryPartialEq};
use crate::io::pages::{GatRefKv, GetGatKvRefSlice, GetKvTxSlice, SubRange};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::iter::Copied;
use std::ops::{Deref, Range, RangeBounds};
use std::ptr::slice_from_raw_parts;
use std::{io, slice};

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

impl<'a, 'tx> GatRefKv<'a> for RefTxBytes<'tx> {
  type RefKv = SharedRefSlice<'a>;
}

impl<'tx> GetGatKvRefSlice for RefTxBytes<'tx> {
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatRefKv<'a>>::RefKv {
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
  type KvTx = RefTxSlice<'tx>;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    let range = (0..self.bytes.len()).sub_range(range);
    RefTxSlice {
      bytes: self.bytes,
      range,
    }
  }
}

impl<'tx> Get<u8> for RefTxSlice<'tx> {
  fn get<'a>(&'a self, index: usize) -> Option<u8> {
    self.bytes.get(index).copied()
  }
}

impl<'tx> TryEq for RefTxSlice<'tx> {}

impl<'tx> KvTryEq for RefTxSlice<'tx> {}

impl<'tx> KvEq for RefTxSlice<'tx> {}

impl<'tx> KvTryOrd for RefTxSlice<'tx> {}

impl<'tx> KvOrd for RefTxSlice<'tx> {}

impl<'tx> KvDataType for RefTxSlice<'tx> {}

impl<'a, 'tx> GatRefKv<'a> for RefTxSlice<'tx> {
  type RefKv = SharedRefSlice<'a>;
}

impl<'tx> GetGatKvRefSlice for RefTxSlice<'tx> {
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatRefKv<'a>>::RefKv {
    SharedRefSlice {
      inner: &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())],
    }
  }
}

impl<'tx> GetKvTxSlice<'tx> for RefTxSlice<'tx> {
  type KvTx = Self;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    let range = self.range.sub_range(range);
    RefTxSlice {
      bytes: self.bytes,
      range,
    }
  }
}

pub struct RefBuf<'a> {
  buf: &'a [u8],
  range: Range<usize>,
}

impl<'a> RefBuf<'a> {
  pub fn new(buf: &'a [u8]) -> Self {
    Self {
      buf,
      range: 0..buf.len(),
    }
  }
}

impl<'a> Buf for RefBuf<'a> {
  fn remaining(&self) -> usize {
    self.range.len()
  }

  fn chunk(&self) -> &[u8] {
    &self.buf[self.range.clone()]
  }

  fn advance(&mut self, cnt: usize) {
    self.range = self.range.sub_range(cnt..);
  }
}

pub struct RefTryBuf<'a> {
  ref_buf: RefBuf<'a>,
}

impl<'a> RefTryBuf<'a> {
  pub fn new(buf: &'a [u8]) -> Self {
    Self {
      ref_buf: RefBuf::new(buf),
    }
  }
}

impl<'a> TryBuf for RefTryBuf<'a> {
  type Error = OpsError;

  fn remaining(&self) -> usize {
    self.ref_buf.remaining()
  }

  fn chunk(&self) -> &[u8] {
    self.ref_buf.chunk()
  }

  fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
    self.ref_buf.advance(cnt);
    Ok(())
  }
}

impl<'tx> Get<u8> for RefTxBytes<'tx> {
  fn get<'a>(&'a self, index: usize) -> Option<u8> {
    self.bytes.get(index).copied()
  }
}

impl<'tx> TryEq for RefTxBytes<'tx> {}

impl<'tx> KvTryEq for RefTxBytes<'tx> {}

impl<'tx> KvEq for RefTxBytes<'tx> {}

impl<'tx> KvTryOrd for RefTxBytes<'tx> {}

impl<'tx> KvOrd for RefTxBytes<'tx> {}

impl<'tx> KvDataType for RefTxBytes<'tx> {}
