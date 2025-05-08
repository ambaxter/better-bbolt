use crate::common::data_pool::UniqueData;
use parking_lot::Mutex;
use size::Size;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicI64, AtomicIsize, Ordering};
use std::{mem, sync};

pub struct UniqueVec<T> {
  pool: Option<VecPool<T>>,
  data: Vec<T>,
}

impl<T> Clone for UniqueVec<T>
where
  T: Clone,
{
  fn clone(&self) -> Self {
    if let Some(pool) = &self.pool {
      let mut vec = pool.pop();
      vec.clone_from_slice(&self.data);
      vec
    } else {
      UniqueVec {
        pool: None,
        data: self.data.clone(),
      }
    }
  }
}

impl<T> Deref for UniqueVec<T> {
  type Target = Vec<T>;
  fn deref(&self) -> &Self::Target {
    &self.data
  }
}

impl<T> DerefMut for UniqueVec<T> {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.data
  }
}

impl<T> PartialEq for UniqueVec<T>
where
  T: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.data == other.data
  }
}

impl<T> Eq for UniqueVec<T> where T: Eq {}

impl<T> PartialOrd for UniqueVec<T>
where
  T: PartialOrd,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self.data.partial_cmp(&other.data)
  }
}

impl<T> Ord for UniqueVec<T>
where
  T: Ord,
{
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.data.cmp(&other.data)
  }
}

impl<T> Hash for UniqueVec<T>
where
  T: Hash,
{
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.data.hash(state);
  }
}

impl<T> Drop for UniqueVec<T> {
  fn drop(&mut self) {
    if let Some(pool) = self.pool.take() {
      let mut swap = Vec::with_capacity(0);
      mem::swap(&mut self.data, &mut swap);
      pool.push(swap);
    }
  }
}

pub struct InnerVecPool<T> {
  init_size: usize,
  min_size: usize,
  max_size: usize,
  pool: Mutex<Vec<Vec<T>>>,
}

impl<T> InnerVecPool<T> {
  #[inline]
  fn len(&self) -> usize {
    self.pool.lock().len()
  }

  fn new_unique(&self) -> UniqueVec<T> {
    UniqueVec {
      pool: None,
      data: vec![],
    }
  }

  fn pop(&self) -> UniqueVec<T> {
    self
      .pool
      .lock()
      .pop()
      .map(|data| UniqueVec { pool: None, data })
      .unwrap_or_else(|| self.new_unique())
  }

  fn push(&self, mut data: Vec<T>) {
    data.clear();
    self.pool.lock().push(data);
  }
}

pub struct VecPool<T> {
  inner: sync::Arc<InnerVecPool<T>>,
}

impl<T> Clone for VecPool<T> {
  fn clone(&self) -> Self {
    VecPool {
      inner: self.inner.clone(),
    }
  }
}

impl<T> VecPool<T> {
  pub fn new(init_size: usize, min_size: usize, max_size: usize) -> Self {
    let mut pool = Vec::with_capacity(init_size);
    for _ in 0..init_size {
      pool.push(Vec::new());
    }
    let inner = InnerVecPool {
      init_size,
      min_size,
      max_size,
      pool: Mutex::new(pool),
    };
    VecPool {
      inner: sync::Arc::new(inner),
    }
  }

  pub fn push(&self, vec: Vec<T>) {
    self.inner.push(vec);
  }

  pub fn pop(&self) -> UniqueVec<T> {
    let mut vec = self.inner.pop();
    vec.pool = Some(self.clone());
    vec
  }
}

impl<T> Debug for VecPool<T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    f.debug_struct("BufferPool")
      .field("init_size", &self.inner.init_size)
      .field("min_size", &self.inner.min_size)
      .field("max_size", &self.inner.max_size)
      .field("len", &self.inner.len())
      .finish()
  }
}
