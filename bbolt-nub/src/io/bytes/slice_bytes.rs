use std::ops::RangeBounds;
use std::slice;
use crate::io::bytes::{FromIOBytes, TxBytes};
use crate::io::bytes::ref_bytes::{RefBytes, RefTxBytes};
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice};
use crate::io::pages::direct::ops::{DirectGet, KvDataType, KvEq, KvOrd};


// TODO: Can we make this work?

impl<'tx> TxBytes<'tx> for &'tx [u8] {}

impl<'tx> FromIOBytes<'tx, RefBytes> for &'tx [u8] {
  fn from_io(value: RefBytes) -> Self {
    unsafe { slice::from_raw_parts(value.ptr, value.len) }
  }
}

impl<'a, 'tx> GatKvRef<'a> for &'tx [u8] {
  type KvRef = &'a [u8];

}

impl<'tx> GetGatKvRefSlice for &'tx [u8] {
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    &self.as_ref()[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

impl<'tx> GetKvTxSlice<'tx> for &'tx [u8] {
  type KvTx = &'tx [u8];

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

impl DirectGet<u8> for [u8] {
  fn direct_get(&self, index: usize) -> Option<u8> {
    self.get(index).copied()
  }
}

impl KvEq for [u8] {}
impl KvOrd for [u8] {}
impl KvDataType for [u8] {}