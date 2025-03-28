use std::iter::Copied;
use std::slice;
use crate::tx_io::bytes::shared_bytes::SharedTxBytes;
use crate::tx_io::pages::{KvDataType, RefIntoCopiedIter};

// &'a [u8] //

impl RefIntoCopiedIter for [u8] {
  type Iter<'a> = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.iter().copied()
  }
}

impl<'tx> KvDataType<'tx> for &'tx [u8] {
  #[inline]
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.eq(other)
  }

  #[inline]
  fn lt(&self, other: &[u8]) -> bool {
    self.lt(other)
  }

  #[inline]
  fn le(&self, other: &[u8]) -> bool {
    self.le(other)
  }

  #[inline]
  fn gt(&self, other: &[u8]) -> bool {
    self.gt(other)
  }

  #[inline]
  fn ge(&self, other: &[u8]) -> bool {
    self.ge(other)
  }
}


// Shared Tx Bytes //


impl<'tx> RefIntoCopiedIter for SharedTxBytes<'tx> {
  type Iter<'a> = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.iter().copied()
  }
}


impl<'tx> KvDataType<'tx> for SharedTxBytes<'tx> {
  #[inline]
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }

  #[inline]
  fn lt(&self, other: &[u8]) -> bool {
    self.as_ref().lt(other)
  }

  #[inline]
  fn le(&self, other: &[u8]) -> bool {
    self.as_ref().le(other)
  }

  #[inline]
  fn gt(&self, other: &[u8]) -> bool {
    self.as_ref().gt(other)
  }

  #[inline]
  fn ge(&self, other: &[u8]) -> bool {
    self.as_ref().ge(other)
  }
}