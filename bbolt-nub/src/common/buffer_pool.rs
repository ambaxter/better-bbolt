use parking_lot::Mutex;
use size::Size;
use std::fmt::Debug;
use std::io;
use std::mem::MaybeUninit;
use std::ops::Deref;
use triomphe::{Arc, ArcBorrow, HeaderSlice, UniqueArc};
use uninit::extension_traits::AsOut;
use uninit::read::ReadIntoUninit;

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

  pub fn read_exact_and_share<R: ReadIntoUninit>(self, r: &mut R) -> io::Result<Arc<PoolBuffer>> {
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
    Ok(unique.shareable())
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

pub struct SharedBufferRef<'a> {
  inner: &'a SharedBuffer,
}

impl<'a> Deref for SharedBufferRef<'a> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.inner.deref()
  }
}

impl<'a> AsRef<[u8]> for SharedBufferRef<'a> {
  fn as_ref(&self) -> &[u8] {
    self.inner.as_ref()
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
      pool.push(BufferPool::create_new(page_size));
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

  pub fn create_new(len: usize) -> UniqueBuffer {
    UniqueArc::from_header_and_uninit_slice(None, len).into()
  }

  // TODO: Can we put this on a different thread?
  fn push(&self, buffer: UniqueArc<PoolBuffer>) {
    self.inner.push(buffer);
  }

  fn pop(&self) -> UniqueBuffer {
    let pool_entry = self.inner.pop();
    let mut buffer = pool_entry.unwrap_or_else(|| BufferPool::create_new(self.inner.page_size));
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
    let pool = BufferPool::create_new(4096);
    let mut empty = vec![0u8; 4096];
    let pool = pool.read_exact_and_share(&mut empty.as_slice()).unwrap();
    assert!(pool.slice.as_ptr().cast::<PageHeader>().is_aligned());
  }
}
