use crate::io::pages::{
  HasRootPage, IntoCopiedIterator, KvDataType, SubRefSlice, SubTxSlice, TxPage,
};
use std::iter::Copied;
use std::ops::RangeBounds;

impl<'tx> SubTxSlice<'tx> for &'tx [u8] {
  type TxSlice = &'tx [u8];
  fn sub_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

impl<'tx> SubRefSlice for &'tx [u8] {
  type RefSlice<'a>
    = &'a [u8]
  where
    Self: 'a;

  fn sub_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefSlice<'a> {
    &self[(range.start_bound().cloned(), range.end_bound().cloned())]
  }
}

impl<'tx> IntoCopiedIterator<'tx> for &'tx [u8] {
  type CopiedIter<'a>
    = Copied<std::slice::Iter<'a, u8>>
  where
    Self: 'a;

  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    self.iter().copied()
  }
}

impl<'a> KvDataType for &'a [u8] {
  #[inline]
  fn partial_eq(&self, other: &[u8]) -> bool {
    PartialEq::eq(*self, other)
  }

  #[inline]
  fn lt(&self, other: &[u8]) -> bool {
    PartialOrd::lt(*self, other)
  }

  #[inline]
  fn le(&self, other: &[u8]) -> bool {
    PartialOrd::le(*self, other)
  }

  #[inline]
  fn gt(&self, other: &[u8]) -> bool {
    PartialOrd::gt(*self, other)
  }

  #[inline]
  fn ge(&self, other: &[u8]) -> bool {
    PartialOrd::ge(*self, other)
  }
}
impl<'tx> HasRootPage for &'tx [u8] {
  fn root_page(&self) -> &[u8] {
    self
  }
}

impl<'tx> TxPage<'tx> for &'tx [u8] {}
