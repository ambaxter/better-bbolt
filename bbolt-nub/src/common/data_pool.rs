use crate::common::buffer_pool::{BufferPool, PoolBuffer, UniqueBuffer};
use crate::io::pages::lazy::ops::TryBuf;
use parking_lot::Mutex;
use rayon;
use size::Size;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

pub type PoolData = triomphe::HeaderSlice<Option<DataPool>, Vec<u8>>;

#[derive(Clone)]
pub struct SharedData {
  pub(crate) inner: Option<triomphe::Arc<PoolData>>,
}

impl Deref for SharedData {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl AsRef<[u8]> for SharedData {
  fn as_ref(&self) -> &[u8] {
    self
      .inner
      .as_ref()
      .expect("shared buffer is dropped")
      .slice
      .as_ref()
  }
}

impl Borrow<[u8]> for SharedData {
  fn borrow(&self) -> &[u8] {
    self.as_ref()
  }
}

impl PartialEq for SharedData {
  fn eq(&self, other: &Self) -> bool {
    self.deref() == other.deref()
  }
}

impl Eq for SharedData {}

impl Ord for SharedData {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.deref().cmp(other.deref())
  }
}

impl PartialOrd for SharedData {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Hash for SharedData {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.deref().hash(state);
  }
}

impl Drop for SharedData {
  fn drop(&mut self) {
    let inner = self.inner.take().expect("shared buffer is dropped");
    rayon::spawn(move || {
      // There is a race condition here, but there's nothing we can do about it
      // https://github.com/Manishearth/triomphe/pull/109
      if let Some(mut unique) = triomphe::Arc::try_unique(inner).ok() {
        if let Some(mut pool) = unique.header.take() {
          pool.push(UniqueData(unique));
        }
      }
    });
  }
}

pub struct UniqueData(triomphe::UniqueArc<PoolData>);

impl UniqueData {
  pub fn set_header(&mut self, header: Option<DataPool>) {
    self.0.header = header;
  }

  pub fn copy_data_and_share(mut self, data: &[u8]) -> SharedData {
    self.0.slice.copy_from_slice(data);
    let shared = self.0.shareable();
    SharedData {
      inner: Some(shared),
    }
  }

  pub fn copy_try_buf_and_share<T: TryBuf>(
    mut self, mut data: T,
  ) -> crate::Result<SharedData, T::Error> {
    let data_len = data.remaining();
    let slice_capacity = self.0.slice.capacity();
    if data_len > slice_capacity {
      self.0.slice.reserve(data_len - slice_capacity);
    }

    while data.remaining() != 0 {
      let chunk_len = data.chunk().len();
      self.0.slice.extend_from_slice(data.chunk());
      data.try_advance(chunk_len)?
    }
    let shared = self.0.shareable();
    Ok(SharedData {
      inner: Some(shared),
    })
  }
}

struct InnerDataPool {
  init_size: Size,
  min_size: Size,
  max_size: Size,
  current_size_in_bytes: AtomicI64,
  default_data_capacity: usize,
  max_data_capacity: usize,
  pool: Mutex<Vec<UniqueData>>,
}

impl InnerDataPool {
  #[inline]
  fn current_size_in_bytes(&self) -> i64 {
    self.current_size_in_bytes.load(Ordering::Acquire)
  }

  fn new_unique(&self) -> UniqueData {
    let data = PoolData {
      header: None,
      slice: Vec::with_capacity(self.default_data_capacity),
    };
    UniqueData(triomphe::UniqueArc::new(data))
  }

  fn pop(&self) -> UniqueData {
    self
      .pool
      .lock()
      .pop()
      .inspect(|data| {
        self
          .current_size_in_bytes
          .fetch_sub(data.0.slice.capacity() as i64, Ordering::Relaxed);
      })
      .unwrap_or_else(|| self.new_unique())
  }

  fn push(&self, mut data: UniqueData) {
    data.0.slice.clear();
    data.0.slice.shrink_to(self.max_data_capacity);
    self
      .current_size_in_bytes
      .fetch_add(data.0.slice.capacity() as i64, Ordering::Relaxed);
    self.pool.lock().push(data);
  }
}

#[derive(Clone)]
pub struct DataPool {
  inner: sync::Arc<InnerDataPool>,
}

impl Debug for DataPool {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("BufferPool")
      .field("init_size", &self.inner.init_size)
      .field("min_size", &self.inner.min_size)
      .field("max_size", &self.inner.max_size)
      .field("current_size_in_bytes", &self.inner.current_size_in_bytes())
      .field("default_data_capacity", &self.inner.default_data_capacity)
      .field("max_data_capacity", &self.inner.max_data_capacity)
      .finish()
  }
}

impl DataPool {
  pub fn pop(&self) -> UniqueData {
    let mut entry = self.inner.pop();
    entry.set_header(Some(self.clone()));
    entry
  }

  fn push(&self, data: UniqueData) {
    self.inner.push(data);
  }
}
