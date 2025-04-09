use crate::io::TxSlot;
use crate::io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::io::ops::{
  KvDataType, KvEq, KvOrd, RefIntoCopiedIter, RefIntoTryBuf, SubRange, TryBuf, TryGet,
};
use std::cmp::Ordering;
use std::iter::Copied;
use std::ops::{Deref, Range};
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
  type Error = io::Error;

  fn remaining(&self) -> usize {
    self.range.len()
  }

  fn chunk(&self) -> &[u8] {
    &self.buf[self.range.clone()]
  }

  fn try_advance(&mut self, cnt: usize) -> Result<(), Self::Error> {
    self.range = self.range.sub_range(cnt..);
    Ok(())
  }
}

impl<'tx> RefIntoTryBuf for RefTxBytes<'tx> {
  type Error = io::Error;
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
    Ok(RefTryBuf::new(self.bytes))
  }
}

impl RefIntoTryBuf for [u8] {
  type Error = io::Error;
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
    Ok(RefTryBuf::new(self))
  }
}

impl<'tx> KvEq for RefTxBytes<'tx> {}
impl<'tx> KvOrd for RefTxBytes<'tx> {}

impl<'tx> TryGet<u8> for RefTxBytes<'tx> {
  type Error = io::Error;

  fn try_get(&self, index: usize) -> Result<Option<u8>, Self::Error> {
    Ok(self.as_ref().get(index).copied())
  }
}

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
