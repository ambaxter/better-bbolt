use crate::io::pages::{IntoCopiedIterator, KvDataType, SubSlice};
use std::iter::Copied;
use std::ops::RangeBounds;

impl<'tx> SubSlice<'tx> for &'tx [u8] {
  type Output = &'tx [u8];
  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::Output {
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

impl<'a> KvDataType<'a> for &'a [u8] {
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

  fn slice_index<R: RangeBounds<usize>>(&self, range: R) -> Self {
    let (start, end) = (range.start_bound().cloned(), range.end_bound().cloned());
    &self[(start, end)]
  }
}
