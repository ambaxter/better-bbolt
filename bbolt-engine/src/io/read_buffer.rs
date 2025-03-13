use aligners::{AlignedBytes, alignment};
use parking_lot::Mutex;
use std::ops::Deref;
use std::sync::Arc;

pub struct ReadBuffer {
  buffer: Option<AlignedBytes<alignment::Page>>,
  return_pool: Option<ReadBufferPool>,
}

impl ReadBuffer {
  pub fn new(buffer: AlignedBytes<alignment::Page>) -> Self {
    ReadBuffer {
      buffer: Some(buffer),
      return_pool: None,
    }
  }

  pub fn with_pool(buffer: AlignedBytes<alignment::Page>, return_pool: ReadBufferPool) -> Self {
    ReadBuffer {
      buffer: Some(buffer),
      return_pool: Some(return_pool),
    }
  }
}

impl Deref for ReadBuffer {
  type Target = [u8];

  #[inline]
  fn deref(&self) -> &Self::Target {
    self.buffer.as_ref().expect("Buffer is empty")
  }
}

impl Drop for ReadBuffer {
  fn drop(&mut self) {
    match (self.buffer.take(), self.return_pool.take()) {
      (Some(buffer), Some(pool)) => {
        pool.inner.stack.lock().push(buffer);
      }
      _ => {}
    }
  }
}

struct ReadBufferPoolInner {
  stack: Mutex<Vec<AlignedBytes<alignment::Page>>>,
  page_size: usize,
}

#[derive(Clone)]
pub struct ReadBufferPool {
  inner: Arc<ReadBufferPoolInner>,
}

impl ReadBufferPool {
  pub fn new(page_size: usize, cap: usize) -> Self {
    let mut stack = Vec::with_capacity(cap);
    for _ in 0..cap {
      stack.push(AlignedBytes::new_zeroed(page_size));
    }

    ReadBufferPool {
      inner: Arc::new(ReadBufferPoolInner {
        stack: Mutex::new(stack),
        page_size,
      }),
    }
  }

  pub fn pop(&self) -> ReadBuffer {
    let top = self.inner.stack.lock().pop();
    top
      .map(|t| ReadBuffer::with_pool(t, self.clone()))
      .unwrap_or_else(|| ReadBuffer::new(AlignedBytes::new_zeroed(self.inner.page_size)))
  }

  pub fn len(&self) -> usize {
    self.inner.stack.lock().len()
  }
}
