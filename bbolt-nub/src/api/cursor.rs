use crate::api::bucket::{BucketApi, MutBucket};
use crate::api::bytes::TxSlice;
use crate::api::errors::DbError;

pub trait CursorApi<'tx>: Sized {
  type KvType: TxSlice<'tx>;

  fn first(&mut self) -> Option<(Self::KvType, Self::KvType)>;
  fn last(&mut self) -> Option<(Self::KvType, Self::KvType)>;
  fn next(&mut self) -> Option<(Self::KvType, Self::KvType)>;
  fn prev(&mut self) -> Option<(Self::KvType, Self::KvType)>;
  fn seek(&mut self, key: &[u8]) -> Option<(Self::KvType, Self::KvType)>;
}

pub trait MutCursorApi<'tx>: CursorApi<'tx> {
  fn delete(&mut self, key: &[u8]) -> Result<(), DbError>;
}

pub trait BucketCursorApi<'tx> {
  type KvType: TxSlice<'tx>;
  type BucketType: BucketApi<'tx, KvType = Self::KvType>;

  fn first(&mut self) -> Option<(Self::KvType, Self::BucketType)>;
  fn last(&mut self) -> Option<(Self::KvType, Self::BucketType)>;
  fn next(&mut self) -> Option<(Self::KvType, Self::BucketType)>;
  fn prev(&mut self) -> Option<(Self::KvType, Self::BucketType)>;
  fn seek(&mut self, key: &[u8]) -> Option<(Self::KvType, Self::BucketType)>;
}

pub trait MutBucketCursorApi<'tx> {
  type KvType: TxSlice<'tx>;
  type BucketType<'a>: MutBucket<'tx, KvType = Self::KvType> + 'a
  where
    Self: 'a;

  fn first<'a>(&'a mut self) -> Option<(Self::KvType, Self::BucketType<'a>)>;
  fn last<'a>(&'a mut self) -> Option<(Self::KvType, Self::BucketType<'a>)>;
  fn next<'a>(&'a mut self) -> Option<(Self::KvType, Self::BucketType<'a>)>;
  fn prev<'a>(&'a mut self) -> Option<(Self::KvType, Self::BucketType<'a>)>;
  fn seek<'a>(&'a mut self, key: &[u8]) -> Option<(Self::KvType, Self::BucketType<'a>)>;
  fn delete(&mut self, key: &[u8]) -> Result<(), DbError>;
}
