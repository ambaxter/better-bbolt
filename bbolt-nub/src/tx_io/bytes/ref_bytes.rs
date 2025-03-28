use crate::tx_io::TxSlot;
use crate::tx_io::bytes::{FromIOBytes, IOBytes, TxBytes};
use crate::tx_io::pages::RefIntoCopiedIter;
use std::ops::Deref;
use std::ptr::slice_from_raw_parts;
use std::slice;

#[derive(Debug, Clone)]
pub struct RefBytes {
  ptr: *const u8,
  len: usize,
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

impl<'tx> TxBytes<'tx> for &'tx [u8] {}

impl<'tx> FromIOBytes<'tx, RefBytes> for &'tx [u8] {
  fn from_io(value: RefBytes) -> &'tx [u8] {
    unsafe { slice::from_raw_parts(value.ptr, value.len) }
  }
}
