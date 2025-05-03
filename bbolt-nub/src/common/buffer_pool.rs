use crate::io::bytes::shared_bytes::SharedBytes;
use parking_lot::Mutex;
use size::Size;
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::Debug;
use std::fs::File;
use std::iter::Copied;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::{Deref, Range, RangeBounds};
use std::sync::atomic::AtomicI64;
use std::{io, sync};
use triomphe::{HeaderSlice, UniqueArc};
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

  pub fn read_exact_and_share<R: ReadIntoUninit>(self, r: &mut R) -> io::Result<SharedBytes> {
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

    Ok(SharedBytes {
      inner: ManuallyDrop::new(shared),
    })
  }

  #[cfg(target_family = "unix")]
  pub fn read_exact_at_and_share(self, file: &File, offset: u64) -> io::Result<SharedBytes> {
    use std::os::unix::fs::FileExt;
    let mut unique = match self {
      UniqueBuffer::Uninit(mut uninit) => {
        uninit.slice.as_out().fill(0);
        unsafe { uninit.assume_init_slice_with_header() }
      }
      UniqueBuffer::Init(init) => init,
    };
    file.read_exact_at(&mut unique.slice, offset)?;
    let shared = unique.shareable();
    Ok(SharedBytes {
      inner: ManuallyDrop::new(shared),
    })
  }
}

struct InnerBufferPool {
  init_size: Size,
  min_size: Size,
  max_size: Size,
  page_size: usize,
  pool: Mutex<Vec<UniqueBuffer>>,
}

impl InnerBufferPool {
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
  inner: sync::Arc<InnerBufferPool>,
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
    let inner = InnerBufferPool {
      init_size,
      min_size,
      max_size,
      page_size,
      pool: Mutex::new(pool),
    };
    BufferPool {
      inner: sync::Arc::new(inner),
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

  pub fn page_size(&self) -> usize {
    self.inner.page_size
  }

  pub(crate) fn push(&self, buffer: UniqueArc<PoolBuffer>) {
    if buffer.slice.len() == self.inner.page_size {
      self.inner.push(buffer);
    }
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
  use crate::common::layout::page::PageHeader;
  use std::alloc::Layout;

  #[test]
  fn test() {
    let pool = BufferPool::new_unbound(4012);
    let mut empty = vec![0u8; 4012];
    let pool = pool.read_exact_and_share(&mut empty.as_slice()).unwrap();
    println!("pool alignment: {:?}", align_of_val(&pool));
    println!("pool size: {:?}", size_of_val(&pool));
    println!("pool.inner alignment: {:?}", align_of_val(&pool.inner));
    println!("pool.inner size: {:?}", size_of_val(&pool.inner));
    let arc = pool.inner.as_ref();
    println!("arc.header alignment: {:?}", align_of_val(&arc.header));
    println!("arc.header size: {:?}", size_of_val(&arc.header));
    println!("arc.slice alignment: {:?}", align_of_val(&arc.slice));
    println!("arc.slice size: {:?}", size_of_val(&arc.slice));
    println!("layout: {:?}", Layout::for_value(arc));
  }
}
