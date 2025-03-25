use crate::common::buffer_pool::PoolBuffer;
use crate::io::pages::{HasRootPage, IntoCopiedIterator, KvDataType, SubRange, SubSlice, TxPage};
use std::cmp::Ordering;
use std::iter::Copied;
use std::ops::{Deref, Range, RangeBounds};
use triomphe::{Arc, UniqueArc};

#[derive(Clone)]
pub struct SharedBuffer {
  pub(crate) inner: Option<Arc<PoolBuffer>>,
}

impl Deref for SharedBuffer {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl AsRef<[u8]> for SharedBuffer {
  fn as_ref(&self) -> &[u8] {
    self
      .inner
      .as_ref()
      .expect("shared buffer is dropped")
      .slice
      .as_ref()
  }
}

impl Drop for SharedBuffer {
  fn drop(&mut self) {
    let inner = self.inner.take().expect("shared buffer is dropped");
    if inner.is_unique() {
      let mut inner: UniqueArc<PoolBuffer> = inner.try_into().expect("shared buffer isn't unique?");
      if let Some(pool) = inner.header.take() {
        pool.push(inner);
      }
    }
  }
}

#[derive(Clone)]
pub struct SharedBufferSlice {
  pub(crate) inner: SharedBuffer,
  pub(crate) range: Range<usize>,
}

impl SharedBufferSlice {
  pub fn new<R: RangeBounds<usize>>(shared: SharedBuffer, range: R) -> Self {
    let range = (0..shared.len()).sub_range(range);

    SharedBufferSlice {
      inner: shared,
      range,
    }
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

impl<'tx> KvDataType<'tx> for SharedBufferSlice {
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

impl<'tx> SubSlice<'tx> for SharedBuffer {
  type OutputSlice = SharedBufferSlice;

  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::OutputSlice {
    SharedBufferSlice::new(self.clone(), range)
  }
}

impl<'tx> SubSlice<'tx> for SharedBufferSlice {
  type OutputSlice = SharedBufferSlice;

  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::OutputSlice {
    SharedBufferSlice::new(self.inner.clone(), self.range.sub_range(range))
  }
}

impl HasRootPage for SharedBuffer {
  fn root_page(&self) -> &[u8] {
    self.as_ref()
  }
}

impl<'tx> TxPage<'tx> for SharedBuffer {}