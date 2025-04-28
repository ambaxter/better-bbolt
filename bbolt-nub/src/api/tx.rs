use crate::api::bucket::{BucketApi, MutBucket};
use crate::api::bytes::TxSlice;
use crate::api::cursor::{BucketCursorApi, MutBucketCursorApi};
use crate::api::errors::DbError;
use crate::common::id::TxId;
use parking_lot::Mutex;
use std::fmt::{Debug, Formatter};
use std::ops::SubAssign;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct TxStats {
  // Page statistics.
  //
  /// number of page allocations
  page_count: AtomicI64,
  /// total bytes allocated
  page_alloc: AtomicI64,

  // Cursor statistics.
  //
  /// number of cursors created
  cursor_count: AtomicI64,

  // Node statistics
  //
  /// number of node allocations
  node_count: AtomicI64,
  /// number of node dereferences
  node_deref: AtomicI64,

  // Rebalance statistics.
  //
  /// number of node rebalances
  rebalance: AtomicI64,
  /// total time spent rebalancing
  rebalance_time: Mutex<Duration>,

  // Split/Spill statistics.
  //
  /// number of nodes split
  split: AtomicI64,
  /// number of nodes spilled
  spill: AtomicI64,
  /// total time spent spilling
  spill_time: Mutex<Duration>,

  // Write statistics.
  //
  /// number of writes performed
  write: AtomicI64,
  /// total time spent writing to disk
  write_time: Mutex<Duration>,
}

impl TxStats {
  /// total bytes allocated
  pub fn page_alloc(&self) -> i64 {
    self.page_alloc.load(Ordering::Acquire)
  }

  pub(crate) fn inc_page_alloc(&self, delta: i64) {
    self.page_alloc.fetch_add(delta, Ordering::Relaxed);
  }

  /// number of page allocations
  pub fn page_count(&self) -> i64 {
    self.page_count.load(Ordering::Acquire)
  }

  pub(crate) fn inc_page_count(&self, delta: i64) {
    self.page_count.fetch_add(delta, Ordering::Relaxed);
  }

  /// number of cursors created
  pub fn cursor_count(&self) -> i64 {
    self.cursor_count.load(Ordering::Acquire)
  }

  pub(crate) fn inc_cursor_count(&self, delta: i64) {
    self.cursor_count.fetch_add(delta, Ordering::Relaxed);
  }

  /// number of node allocations
  pub fn node_count(&self) -> i64 {
    self.node_count.load(Ordering::Acquire)
  }

  pub(crate) fn inc_node_count(&self, delta: i64) {
    self.node_count.fetch_add(delta, Ordering::Relaxed);
  }

  /// number of node dereferences
  pub fn node_deref(&self) -> i64 {
    self.node_deref.load(Ordering::Acquire)
  }

  pub(crate) fn inc_node_deref(&self, delta: i64) {
    self.node_deref.fetch_add(delta, Ordering::Relaxed);
  }

  /// number of node rebalances
  pub fn rebalance(&self) -> i64 {
    self.rebalance.load(Ordering::Acquire)
  }

  pub(crate) fn inc_rebalance(&self, delta: i64) {
    self.rebalance.fetch_add(delta, Ordering::Relaxed);
  }

  /// total time spent rebalancing
  pub fn rebalance_time(&self) -> Duration {
    *self.rebalance_time.lock()
  }

  pub(crate) fn inc_rebalance_time(&self, delta: Duration) {
    *self.rebalance_time.lock() += delta;
  }

  /// number of nodes split
  pub fn split(&self) -> i64 {
    self.split.load(Ordering::Acquire)
  }

  pub(crate) fn inc_split(&self, delta: i64) {
    self.split.fetch_add(delta, Ordering::Relaxed);
  }

  /// number of nodes spilled
  pub fn spill(&self) -> i64 {
    self.spill.load(Ordering::Acquire)
  }

  pub(crate) fn inc_spill(&self, delta: i64) {
    self.spill.fetch_add(delta, Ordering::Relaxed);
  }

  /// total time spent spilling
  pub fn spill_time(&self) -> Duration {
    *self.spill_time.lock()
  }

  pub(crate) fn inc_spill_time(&self, delta: Duration) {
    *self.spill_time.lock() += delta;
  }

  /// number of writes performed
  pub fn write(&self) -> i64 {
    self.write.load(Ordering::Acquire)
  }

  pub(crate) fn inc_write(&self, delta: i64) {
    self.write.fetch_add(delta, Ordering::Relaxed);
  }

  /// total time spent writing to disk
  pub fn write_time(&self) -> Duration {
    *self.write_time.lock()
  }

  pub(crate) fn inc_write_time(&self, delta: Duration) {
    *self.write_time.lock() += delta;
  }

  pub(crate) fn add_assign(&self, rhs: &TxStats) {
    self.inc_page_count(rhs.page_count());
    self.inc_page_alloc(rhs.page_alloc());
    self.inc_cursor_count(rhs.cursor_count());
    self.inc_node_count(rhs.node_count());
    self.inc_node_deref(rhs.node_deref());
    self.inc_rebalance(rhs.rebalance());
    self.inc_rebalance_time(rhs.rebalance_time());
    self.inc_split(rhs.split());
    self.inc_spill(rhs.spill());
    self.inc_spill_time(rhs.spill_time());
    self.inc_write(rhs.write());
    self.inc_write_time(rhs.write_time());
  }

  pub(crate) fn add(&self, rhs: &TxStats) -> TxStats {
    let add = self.clone();
    add.add_assign(rhs);
    add
  }

  pub(crate) fn sub_assign(&self, rhs: &TxStats) {
    self.inc_page_count(-rhs.page_count());
    self.inc_page_alloc(-rhs.page_alloc());
    self.inc_cursor_count(-rhs.cursor_count());
    self.inc_node_count(-rhs.node_count());
    self.inc_node_deref(-rhs.node_deref());
    self.inc_rebalance(-rhs.rebalance());
    self.rebalance_time.lock().sub_assign(rhs.rebalance_time());
    self.inc_split(-rhs.split());
    self.inc_spill(-rhs.spill());
    self.spill_time.lock().sub_assign(rhs.spill_time());
    self.inc_write(-rhs.write());
    self.write_time.lock().sub_assign(rhs.write_time());
  }

  pub(crate) fn sub(&self, rhs: &TxStats) -> TxStats {
    let sub = self.clone();
    sub.sub_assign(rhs);
    sub
  }
}

impl Clone for TxStats {
  fn clone(&self) -> Self {
    TxStats {
      page_count: self.page_count().into(),
      page_alloc: self.page_alloc().into(),
      cursor_count: self.cursor_count().into(),
      node_count: self.node_count().into(),
      node_deref: self.node_deref().into(),
      rebalance: self.rebalance().into(),
      rebalance_time: self.rebalance_time().into(),
      split: self.split().into(),
      spill: self.spill().into(),
      spill_time: self.spill_time().into(),
      write: self.write().into(),
      write_time: self.write_time().into(),
    }
  }
}

impl PartialEq for TxStats {
  fn eq(&self, other: &Self) -> bool {
    self.page_count() == other.page_count()
      && self.page_alloc() == other.page_alloc()
      && self.cursor_count() == other.cursor_count()
      && self.node_count() == other.node_count()
      && self.node_deref() == other.node_deref()
      && self.rebalance() == other.rebalance()
      && self.rebalance_time() == other.rebalance_time()
      && self.split() == other.split()
      && self.spill() == other.spill()
      && self.spill_time() == other.spill_time()
      && self.write() == other.write()
      && self.write_time() == other.write_time()
  }
}

impl Eq for TxStats {}

impl Debug for TxStats {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("TxStats")
      .field("page_count", &self.page_count())
      .field("page_alloc", &self.page_alloc())
      .field("cursor_count", &self.cursor_count())
      .field("node_count", &self.node_count())
      .field("node_deref", &self.node_deref())
      .field("rebalance", &self.rebalance())
      .field("rebalance_time", &self.rebalance_time())
      .field("split", &self.split())
      .field("spill", &self.spill())
      .field("spill_time", &self.spill_time())
      .field("write", &self.write())
      .field("write_time", &self.write_time())
      .finish()
  }
}

pub trait TxApi<'db>: Sized {
  type SliceType<'tx>: TxSlice<'tx>
  where
    Self: 'tx;
  type BucketType<'tx>: BucketApi<'tx, KvType = Self::SliceType<'tx>>
  where
    Self: 'tx;
  type BucketCursorType<'tx>: BucketCursorApi<'tx, KvType = Self::SliceType<'tx>>
  where
    Self: 'tx;

  fn tx_id(&self) -> TxId;

  fn writable(&self) -> bool;

  fn size(&self) -> u64;

  fn bucket<'tx>(&'tx self, path: &[&[u8]]) -> Option<Self::BucketType<'tx>>;

  fn bucket_cursor<'tx>(&'tx self, path: &[&[u8]]) -> Option<Self::BucketCursorType<'tx>>;
}

pub trait MutTx<'db>: TxApi<'db> {
  type MutBucketType<'tx>: MutBucket<'tx, KvType = Self::SliceType<'tx>>
  where
    Self: 'tx;
  type MutBucketCursorType<'tx>: MutBucketCursorApi<'tx, KvType = Self::SliceType<'tx>>
  where
    Self: 'tx;

  fn copy_bucket(&mut self, from: &[&[u8]], to: &[&[u8]]) -> Result<(), DbError>;

  fn delete_bucket(&mut self, path: &[&[u8]]) -> Result<(), DbError>;

  fn bucket_mut<'tx>(&'tx mut self, path: &[&[u8]]) -> Option<Self::MutBucketType<'tx>>;

  fn upsert_bucket<'tx>(&'tx mut self, path: &[&[u8]])
  -> Result<Self::MutBucketType<'tx>, DbError>;

  fn bucket_cursor_mut<'tx>(&mut self, path: &[&[u8]]) -> Option<Self::MutBucketCursorType<'tx>>;

  fn on_commit<'tx, F: FnMut() + 'tx>(&'tx mut self, f: F);

  fn rollback(self) -> Result<(), DbError>;

  fn commit(self) -> Result<(), DbError>;
}
