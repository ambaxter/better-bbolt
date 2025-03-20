use crate::api::bucket::{Bucket, MutBucket};
use crate::api::bytes::TxSlice;
use crate::api::errors::DbError;

pub trait Cursor<'tx>: Sized {
  type SliceType: TxSlice<'tx>;

  fn first(&mut self) -> Option<(Self::SliceType, Self::SliceType)>;
  fn last(&mut self) -> Option<(Self::SliceType, Self::SliceType)>;
  fn next(&mut self) -> Option<(Self::SliceType, Self::SliceType)>;
  fn prev(&mut self) -> Option<(Self::SliceType, Self::SliceType)>;
  fn seek(&mut self, key: &[u8]) -> Option<(Self::SliceType, Self::SliceType)>;
}

pub trait MutCursor<'tx>: Cursor<'tx> {
  fn delete(&mut self, key: &[u8]) -> Result<(), DbError>;
}

pub trait BucketCursor<'tx> {
  type SliceType: TxSlice<'tx>;
  type BucketType: Bucket<'tx, SliceType = Self::SliceType>;

  fn first(&mut self) -> Option<(Self::SliceType, Self::BucketType)>;
  fn last(&mut self) -> Option<(Self::SliceType, Self::BucketType)>;
  fn next(&mut self) -> Option<(Self::SliceType, Self::BucketType)>;
  fn prev(&mut self) -> Option<(Self::SliceType, Self::BucketType)>;
  fn seek(&mut self, key: &[u8]) -> Option<(Self::SliceType, Self::BucketType)>;
}

pub trait MutBucketCursor<'tx> {
  type SliceType: TxSlice<'tx>;
  type BucketType<'a>: MutBucket<'tx, SliceType = Self::SliceType> + 'a
  where
    Self: 'a;

  fn first<'a>(&'a mut self) -> Option<(Self::SliceType, Self::BucketType<'a>)>;
  fn last<'a>(&'a mut self) -> Option<(Self::SliceType, Self::BucketType<'a>)>;
  fn next<'a>(&'a mut self) -> Option<(Self::SliceType, Self::BucketType<'a>)>;
  fn prev<'a>(&'a mut self) -> Option<(Self::SliceType, Self::BucketType<'a>)>;
  fn seek<'a>(&'a mut self, key: &[u8]) -> Option<(Self::SliceType, Self::BucketType<'a>)>;
  fn delete(&mut self, key: &[u8]) -> Result<(), DbError>;
}
