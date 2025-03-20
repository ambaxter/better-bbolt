use crate::api::bucket::{Bucket, MutBucket};
use crate::api::bytes::TxSlice;
use crate::api::cursor::{BucketCursor, MutBucketCursor};
use crate::api::errors::DbError;
use crate::common::id::TxId;
use triomphe::Arc;

#[derive(Clone)]
pub struct TxStats {
  inner: Arc<TxStatsInner>,
}

struct TxStatsInner {}

pub trait Tx<'db>: Sized {
  type SliceType<'tx>: TxSlice<'tx>
  where
    Self: 'tx;
  type BucketType<'tx>: Bucket<'tx, SliceType = Self::SliceType<'tx>>
  where
    Self: 'tx;
  type BucketCursorType<'tx>: BucketCursor<'tx, SliceType = Self::SliceType<'tx>>
  where
    Self: 'tx;

  fn tx_id(&self) -> TxId;

  fn writable(&self) -> bool;

  fn size(&self) -> u64;

  fn bucket<'tx>(&'tx self, path: &[&[u8]]) -> Option<Self::BucketType<'tx>>;

  fn bucket_cursor<'tx>(&'tx self, path: &[&[u8]]) -> Option<Self::BucketCursorType<'tx>>;
}

pub trait MutTx<'db>: Tx<'db> {
  type MutBucketType<'tx>: MutBucket<'tx, SliceType = Self::SliceType<'tx>>
  where
    Self: 'tx;
  type MutBucketCursorType<'tx>: MutBucketCursor<'tx, SliceType = Self::SliceType<'tx>>
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
