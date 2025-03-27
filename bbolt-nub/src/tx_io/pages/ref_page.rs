use crate::tx_io::pages::{FromIO, IOBytes, IntoTx, TxBytes, TxSlot};
use std::io::Read;
use std::ops::Deref;
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

#[derive(Debug, Clone)]
pub struct RefTxBytes<'tx> {
  tx: TxSlot<'tx>,
  bytes: RefBytes,
}

impl<'tx> AsRef<[u8]> for RefTxBytes<'tx> {
  fn as_ref(&self) -> &[u8] {
    self.bytes.as_ref()
  }
}

impl<'tx> Deref for RefTxBytes<'tx> {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl<'tx> TxBytes<'tx> for RefTxBytes<'tx> {}

impl<'tx> FromIO<'tx, RefBytes> for RefTxBytes<'tx> {
  fn from_io(value: RefBytes) -> RefTxBytes<'tx> {
    RefTxBytes {
      tx: Default::default(),
      bytes: value,
    }
  }
}

impl<'tx, U, T> IntoTx<'tx, U> for T
where
  T: IOBytes,
  U: FromIO<'tx, T>,
{
  fn into_tx(self) -> U {
    U::from_io(self)
  }
}

trait IO {
  type Bytes: IOBytes;

  fn read(&self) -> Self::Bytes;
}

trait IOWrapper<'tx, I: IO> {
  type Output: FromIO<'tx, I::Bytes>;
  fn read(&self) -> Self::Output;
}
