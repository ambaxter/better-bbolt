use crate::common::buffer_pool::PoolBuffer;
use crate::io::pages::{
  HasRootPage, IntoCopiedIterator, KvDataType, RefIntoCopiedIterator, SubRange, SubTxSlice, TxPage,
};
use crate::tx_io::bytes::shared_bytes::SharedBytes;
use std::cmp::Ordering;
use std::iter::Copied;
use std::ops::{Deref, Range, RangeBounds};
use triomphe::{Arc, ArcBorrow, UniqueArc};

#[derive(Clone)]
pub struct SharedBufferSlice {
  pub(crate) inner: SharedBytes,
  pub(crate) range: Range<usize>,
}

impl SharedBufferSlice {
  pub fn new<R: RangeBounds<usize>>(shared: SharedBytes, range: R) -> Self {
    let range = (0..shared.len()).sub_range(range);

    SharedBufferSlice { inner: shared, range }
  }
}

impl AsRef<[u8]> for SharedBufferSlice {
  fn as_ref(&self) -> &[u8] {
    &self.inner.as_ref()[self.range.start..self.range.end]
  }
}

impl Ord for SharedBufferSlice {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl Eq for SharedBufferSlice {}

impl PartialEq<Self> for SharedBufferSlice {
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl PartialOrd<Self> for SharedBufferSlice {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }
}

impl PartialEq<[u8]> for SharedBufferSlice {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl PartialOrd<[u8]> for SharedBufferSlice {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
  }
}

impl<'tx> IntoCopiedIterator<'tx> for SharedBufferSlice {
  type CopiedIter<'a>
    = Copied<std::slice::Iter<'a, u8>>
  where
    Self: 'a,
    'tx: 'a;
  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    self.as_ref().iter().copied()
  }
}

impl<'tx> KvDataType for SharedBufferSlice {
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.iter_copied().eq(other.iter_copied())
  }

  fn lt(&self, other: &[u8]) -> bool {
    self.iter_copied().lt(other.iter_copied())
  }

  fn le(&self, other: &[u8]) -> bool {
    self.iter_copied().le(other.iter_copied())
  }

  fn gt(&self, other: &[u8]) -> bool {
    self.iter_copied().gt(other.iter_copied())
  }

  fn ge(&self, other: &[u8]) -> bool {
    self.iter_copied().ge(other.iter_copied())
  }
}

impl<'tx> SubTxSlice<'tx> for SharedBytes {
  type TxSlice = SharedBufferSlice;

  fn sub_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice {
    SharedBufferSlice::new(self.clone(), range)
  }
}

impl<'tx> SubTxSlice<'tx> for SharedBufferSlice {
  type TxSlice = SharedBufferSlice;

  fn sub_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice {
    SharedBufferSlice::new(self.inner.clone(), self.range.sub_range(range))
  }
}

impl HasRootPage for SharedBytes {
  fn root_page(&self) -> &[u8] {
    self.as_ref()
  }
}

impl<'tx> TxPage<'tx> for SharedBytes {}

pub struct SharedRefSlice<'a> {
  pub(crate) inner: &'a Arc<PoolBuffer>,
  pub(crate) range: Range<usize>,
}

impl<'a> AsRef<[u8]> for SharedRefSlice<'a> {
  fn as_ref(&self) -> &[u8] {
    &self.inner.slice.as_ref()[self.range.start..self.range.end]
  }
}

impl<'a> Eq for SharedRefSlice<'a> {}

impl<'a> PartialEq for SharedRefSlice<'a> {
  fn eq(&self, other: &Self) -> bool {
    self.as_ref().eq(other.as_ref())
  }
}

impl<'a> Ord for SharedRefSlice<'a> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl<'a> PartialOrd for SharedRefSlice<'a> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }

  fn lt(&self, other: &Self) -> bool {
    self.as_ref().lt(other.as_ref())
  }
  fn le(&self, other: &Self) -> bool {
    self.as_ref().le(other.as_ref())
  }
  fn gt(&self, other: &Self) -> bool {
    self.as_ref().gt(other.as_ref())
  }
  fn ge(&self, other: &Self) -> bool {
    self.as_ref().ge(other.as_ref())
  }
}

impl<'p> RefIntoCopiedIterator for SharedRefSlice<'p> {
  type RefCopiedIter<'a>
    = Copied<std::slice::Iter<'a, u8>>
  where
    Self: 'a;

  fn ref_iter_copied<'a>(&'a self) -> Self::RefCopiedIter<'a> {
    self.as_ref().iter().copied()
  }
}

impl<'a> KvDataType for SharedRefSlice<'a> {
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.as_ref().partial_eq(other)
  }

  fn lt(&self, other: &[u8]) -> bool {
    self.as_ref().lt(other)
  }

  fn le(&self, other: &[u8]) -> bool {
    self.as_ref().le(other)
  }

  fn gt(&self, other: &[u8]) -> bool {
    self.as_ref().gt(other)
  }

  fn ge(&self, other: &[u8]) -> bool {
    self.as_ref().ge(other)
  }
}
