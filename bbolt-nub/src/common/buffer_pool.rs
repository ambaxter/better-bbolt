use crate::pages::bytes::{TxPage, TxPageSlice};
use parking_lot::Mutex;
use size::Size;
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Debug;
use std::io;
use std::mem::MaybeUninit;
use std::ops::{Deref, Range, RangeBounds};
use triomphe::{Arc, HeaderSlice, UniqueArc};
use uninit::extension_traits::AsOut;
use uninit::read::ReadIntoUninit;
use crate::pages::txpage::IntoCopiedIterator;

pub type PoolMaybeUninitBuffer = HeaderSlice<Option<BufferPool>, [MaybeUninit<u8>]>;
pub type PoolBuffer = HeaderSlice<Option<BufferPool>, [u8]>;

pub enum UniqueBuffer {
  Uninit(UniqueArc<PoolMaybeUninitBuffer>),
  Init(UniqueArc<PoolBuffer>),
}

impl From<UniqueArc<PoolMaybeUninitBuffer>> for UniqueBuffer {
  fn from(value: UniqueArc<PoolMaybeUninitBuffer>) -> Self {
    UniqueBuffer::Uninit(value)
  }
}

impl From<UniqueArc<PoolBuffer>> for UniqueBuffer {
  fn from(value: UniqueArc<PoolBuffer>) -> Self {
    UniqueBuffer::Init(value)
  }
}

impl UniqueBuffer {
  pub fn set_header(&mut self, header: Option<BufferPool>) {
    match self {
      UniqueBuffer::Uninit(uninit) => uninit.header = header,
      UniqueBuffer::Init(init) => init.header = header,
    }
  }

  pub fn read_exact_and_share<R: ReadIntoUninit>(self, r: &mut R) -> io::Result<SharedBuffer> {
    let unique = match self {
      UniqueBuffer::Uninit(mut uninit) => {
        r.read_into_uninit_exact(uninit.slice.as_out())?;
        unsafe { uninit.assume_init_slice_with_header() }
      }
      UniqueBuffer::Init(mut init) => {
        r.read_into_uninit_exact(init.slice.as_out())?;
        init
      }
    };
    let shared = unique.shareable();

    Ok(SharedBuffer {
      inner: Some(shared),
    })
  }
}

#[derive(Clone)]
pub struct SharedBuffer {
  inner: Option<Arc<PoolBuffer>>,
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

impl<'tx> TxPage<'tx> for SharedBuffer {
  type TxSlice = SharedBufferSlice;

  //TODO: Test!
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice {
    let start = match range.start_bound().cloned() {
      Bound::Included(included) => included,
      Bound::Excluded(excluded) => excluded + 1,
      Bound::Unbounded => 0,
    };
    let end = match range.end_bound().cloned() {
      Bound::Included(included) => self.len() - (self.len() - included) + 1,
      Bound::Excluded(excluded) => self.len() - (self.len() - excluded),
      Bound::Unbounded => self.len(),
    };
    SharedBufferSlice {
      inner: self.clone(),
      range: start..end,
    }
  }
}

#[derive(Clone)]
pub struct SharedBufferSlice {
  inner: SharedBuffer,
  range: Range<usize>,
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

impl IntoCopiedIterator for SharedBufferSlice {
  fn iter_copied(&self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator {
    self.as_ref().iter().copied()
  }
}

impl<'tx> TxPageSlice<'tx> for SharedBufferSlice {
  // TODO: Test!!
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self {
    let start = match range.start_bound().cloned() {
      Bound::Included(included) => self.range.start + included,
      Bound::Excluded(excluded) => self.range.start + excluded + 1,
      Bound::Unbounded => self.range.start,
    };
    let end = match range.end_bound().cloned() {
      Bound::Included(included) => self.range.end - (self.range.end - included) + 1,
      Bound::Excluded(excluded) => self.range.end - (self.range.end - excluded),
      Bound::Unbounded => self.range.end,
    };
    SharedBufferSlice {
      inner: self.inner.clone(),
      range: start..end,
    }
  }
}

struct BufferPoolInner {
  init_size: Size,
  min_size: Size,
  max_size: Size,
  page_size: usize,
  pool: Mutex<Vec<UniqueBuffer>>,
}

impl BufferPoolInner {
  fn pop(&self) -> Option<UniqueBuffer> {
    self.pool.lock().pop()
  }

  #[inline]
  fn buffer_size(&self) -> Size {
    Size::from_bytes(self.page_size)
  }

  #[inline]
  fn current_size(&self) -> Size {
    Size::from_bytes(self.pool.lock().len() * self.page_size)
  }

  fn push(&self, buffer: UniqueArc<PoolBuffer>) {
    if buffer.slice.len() == self.page_size {
      let mut pool = self.pool.lock();
      let current_size = Size::from_bytes(pool.len() * self.page_size);
      if current_size + self.buffer_size() <= self.max_size {
        pool.push(buffer.into());
      }
    }
  }

  fn clear_to_min(&self) {
    while self.current_size() > self.min_size {
      self.pool.lock().pop();
    }
  }
}

#[derive(Clone)]
pub struct BufferPool {
  inner: Arc<BufferPoolInner>,
}

impl Debug for BufferPool {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("BufferPool")
      .field("init_size", &self.inner.init_size)
      .field("min_size", &self.inner.min_size)
      .field("max_size", &self.inner.max_size)
      .field("page_size", &self.inner.page_size)
      .field("current_size", &self.inner.current_size())
      .finish()
  }
}

impl BufferPool {
  pub fn new(page_size: usize, init_size: Size, min_size: Size, max_size: Size) -> Self {
    let reserve_size = init_size.bytes() as usize / page_size;
    let mut pool = Vec::with_capacity(reserve_size);
    for _ in 0..reserve_size {
      pool.push(BufferPool::new_unbound(page_size));
    }
    let inner = BufferPoolInner {
      init_size,
      min_size,
      max_size,
      page_size,
      pool: Mutex::new(pool),
    };
    BufferPool {
      inner: Arc::new(inner),
    }
  }

  pub fn new_unbound(len: usize) -> UniqueBuffer {
    UniqueArc::from_header_and_uninit_slice(None, len).into()
  }

  pub fn pop_with_len(&self, len: usize) -> UniqueBuffer {
    if len == self.inner.page_size {
      self.pop()
    } else {
      BufferPool::new_unbound(len)
    }
  }

  // TODO: Can we put this on a different thread?
  fn push(&self, buffer: UniqueArc<PoolBuffer>) {
    self.inner.push(buffer);
  }

  pub fn pop(&self) -> UniqueBuffer {
    let pool_entry = self.inner.pop();
    let mut buffer = pool_entry.unwrap_or_else(|| BufferPool::new_unbound(self.inner.page_size));
    buffer.set_header(Some(self.clone()));
    buffer
  }
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::common::page::PageHeader;

  #[test]
  fn test() {
    let pool = BufferPool::new_unbound(4096);
    let mut empty = vec![0u8; 4096];
    let pool = pool.read_exact_and_share(&mut empty.as_slice()).unwrap();
    assert!(
      pool
        .inner
        .as_ref()
        .unwrap()
        .slice
        .as_ptr()
        .cast::<PageHeader>()
        .is_aligned()
    );
  }
}
