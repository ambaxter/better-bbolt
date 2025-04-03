use crate::Result;
use crate::api::bytes::TxSlice;
use crate::api::cursor::{CursorApi, MutCursorApi};
use crate::api::errors::DbError;
use triomphe::Arc;

#[derive(Clone)]
pub struct BucketStats {
  inner: Arc<BucketStatsInner>,
}

struct BucketStatsInner {}

pub trait BucketApi<'tx>: Sized {
  type KvType: TxSlice<'tx>;

  type CursorType<'a>: CursorApi<'tx, KvType= Self::KvType> + 'a
  where
    Self: 'a;
  fn cursor<'a>(&'a self) -> Self::CursorType<'a>;

  fn sequence(&self) -> u64;

  fn writable(&self) -> bool;

  fn get(&self, key: &[u8]) -> Option<Self::KvType>;

  fn stats(&self) -> BucketStats;
}

pub trait MutBucket<'tx>: BucketApi<'tx> {
  type MutCursorType<'a>: MutCursorApi<'tx, KvType= Self::KvType> + 'a
  where
    Self: 'a;

  fn cursor_mut<'a>(&'a mut self) -> Self::MutCursorType<'a>;
  fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), DbError>;
  fn delete(&mut self, key: &[u8]) -> Result<(), DbError>;
  fn set_sequence(&mut self, sequence: u64) -> Result<(), DbError>;

  fn next_sequence(&mut self) -> Result<u64, DbError>;

  fn set_fill_percent(&mut self, percentage: f64) -> Result<(), DbError>;
}
