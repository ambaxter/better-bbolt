use crate::common::ids::PageId;
use parking_lot::{Condvar, Mutex};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::BufReader;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;

struct PoolEntry<L, T> {
  location: L,
  data: T,
}

impl<L, T> PoolEntry<L, T> {
  pub fn new(location: L, data: T) -> Self {
    Self { location, data }
  }
}

impl<L, T> PartialEq for PoolEntry<L, T>
where
  L: PartialEq,
{
  fn eq(&self, other: &Self) -> bool {
    self.location == other.location
  }
}

impl<L, T> Eq for PoolEntry<L, T> where L: Eq {}

impl<L, T> PartialOrd for PoolEntry<L, T>
where
  L: Ord,
{
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.location.cmp(&other.location))
  }
}

impl<L, T> Ord for PoolEntry<L, T>
where
  L: Ord,
{
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.location.cmp(&other.location)
  }
}

pub struct IOPool<L, T> {
  pool_condvar: Arc<(Mutex<Vec<PoolEntry<L, T>>>, Condvar)>,
}

impl<L, T> Clone for IOPool<L, T> {
  fn clone(&self) -> Self {
    IOPool {
      pool_condvar: self.pool_condvar.clone(),
    }
  }
}

impl<L, T> IOPool<L, T>
where
  L: Ord + Eq + Copy,
{
  pub fn new() -> IOPool<L, T> {
    IOPool {
      pool_condvar: Arc::new((Mutex::new(vec![]), Condvar::new())),
    }
  }

  pub fn insert(&self, location: L, data: T) {
    let &(ref pool, ref cvar) = &*self.pool_condvar;
    let mut pool = pool.lock();
    let closest = pool
      .binary_search_by_key(&location, |entry| entry.location)
      .unwrap_or_else(|i| i);
    let entry = PoolEntry::new(location, data);
    pool.insert(closest, entry);
    cvar.notify_one();
  }

  pub fn access<R, F: FnMut(&mut T) -> io::Result<R>>(
    &self, location: &L, mut f: F,
  ) -> io::Result<R> {
    let &(ref pool, ref cvar) = &*self.pool_condvar;
    let mut entry = {
      let mut pool = pool.lock();
      if pool.is_empty() {
        cvar.wait_while(&mut pool, |pool| pool.is_empty())
      }
      let closest = pool
        .binary_search_by_key(location, |entry| entry.location)
        .unwrap_or_else(|i| i);
      pool.remove(closest)
    };
    let r = f(&mut entry.data);
    {
      entry.location = *location;
      let mut pool = pool.lock();
      let closest = pool
        .binary_search_by_key(location, |entry| entry.location)
        .unwrap_or_else(|i| i);
      pool.insert(closest, entry);
      cvar.notify_one();
    }
    r
  }
}

#[derive(Clone)]
pub struct WritePool {
  pool: IOPool<PageId, File>,
}

impl WritePool {
  pub fn new<P: AsRef<Path>>(path: P, capacity: NonZeroUsize) -> io::Result<WritePool> {
    let pool = IOPool::new();
    let path = path.as_ref();
    for _ in 0..capacity.get() {
      let file = OpenOptions::new().read(true).open(path)?;
      pool.insert(PageId::of(0), file);
    }
    Ok(WritePool { pool })
  }

  pub fn write<R, F: FnMut(&mut File) -> io::Result<R>>(
    &self, location: &PageId, f: F,
  ) -> io::Result<R> {
    self.pool.access(location, f)
  }
}

#[derive(Clone)]
pub struct ReadPool {
  pool: IOPool<PageId, BufReader<File>>,
}

impl ReadPool {
  pub fn new<P: AsRef<Path>>(path: P, capacity: NonZeroUsize) -> io::Result<ReadPool> {
    let pool = IOPool::new();
    let path = path.as_ref();
    for _ in 0..capacity.get() {
      let file = OpenOptions::new().read(true).open(path)?;
      pool.insert(PageId::of(0), BufReader::new(file));
    }
    Ok(ReadPool { pool })
  }

  pub fn read<R, F: FnMut(&mut BufReader<File>) -> io::Result<R>>(
    &self, location: &PageId, f: F,
  ) -> io::Result<R> {
    self.pool.access(location, f)
  }
}
