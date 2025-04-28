use crate::common::data_pool::SharedData;
use crate::common::errors::{BucketError, CursorError};
use crate::common::layout::bucket::BucketHeader;
use crate::common::layout::node::LeafFlag;
use crate::components::cursor::{
  CoreCursor, CoreCursorApi, CoreCursorSeekApi, LeafFlagFilterCursor, StackEntry,
};
use crate::components::tx::{TheMutTx, TheTx};
use crate::io::pages::lazy::ops::TryPartialOrd;
use crate::io::pages::types::node::{HasKeys, NodePage};
use crate::io::pages::{GatKvRef, GetKvTxSlice, TxPageType};
use parking_lot::{Mutex, MutexGuard};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;
use std::ops::Deref;
use std::sync;

pub struct OnDiskBucket<B, L, TX> {
  pub(crate) tx: sync::Arc<TX>,
  pub(crate) header: BucketHeader,
  pub(crate) root: NodePage<B, L>,
}

impl<B, L, TX> OnDiskBucket<B, L, TX> {
  fn sequence(&self) -> u64 {
    self.header.sequence()
  }
}

impl<'tx, TX> OnDiskBucket<TX::BranchType, TX::LeafType, TX>
where
  TX: TheTx<'tx>,
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
{
  fn get(
    &self, key: &[u8],
  ) -> crate::Result<Option<<TX::LeafType as HasKeys<'tx>>::TxKv>, BucketError> {
    let core_cursor = CoreCursor::new(self);
    let mut c = LeafFlagFilterCursor::new(core_cursor, LeafFlag::default());
    match c.seek(key) {
      Ok(v) => Ok(v.map(|_| c.value()).flatten()),
      Err(err) => {
        let e = match err.current_context() {
          CursorError::ValueIsABucket => err.change_context(BucketError::ValueIsABucket),
          _ => err.change_context(BucketError::GetError),
        };
        Err(e)
      }
    }
  }
}

impl<'tx, TX> OnDiskBucket<TX::BranchType, TX::LeafType, TX>
where
  TX: TheTx<'tx>,
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: TryPartialOrd<[u8]>,
{
  fn try_get(
    &self, key: &[u8],
  ) -> crate::Result<<TX::LeafType as HasKeys<'tx>>::TxKv, BucketError> {
    todo!()
  }
}

pub enum ValueDelta {
  UValue(SharedData),
  UBucket(SharedData),
  Delete,
}

pub enum BucketType<B, L, T> {
  OnDisk(OnDiskBucket<B, L, T>),
  Delta(sync::Arc<T>),
}

impl<B, L, T> BucketType<B, L, T> {
  fn tx(&self) -> &T {
    match self {
      BucketType::OnDisk(bucket) => &bucket.tx,
      BucketType::Delta(tx) => tx,
    }
  }
}

#[derive(Clone)]
pub struct BucketDelta {
  delta: sync::Arc<Mutex<BTreeMap<SharedData, ValueDelta>>>,
}

pub struct DeltaBucket<B, L, T> {
  pub(crate) bucket_type: BucketType<B, L, T>,
  pub(crate) delta: BucketDelta,
}

pub enum DeltaKv<D> {
  OnDisk(D),
  Delta(SharedData),
}

impl<D> AsRef<[u8]> for DeltaKv<D>
where
  D: AsRef<[u8]>,
{
  fn as_ref(&self) -> &[u8] {
    match self {
      DeltaKv::OnDisk(d) => d.as_ref(),
      DeltaKv::Delta(u) => u.as_ref(),
    }
  }
}

impl<D> Deref for DeltaKv<D>
where
  D: AsRef<[u8]>,
{
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl<'tx, B, L, TX> DeltaBucket<B, L, TX>
where
  TX: TheMutTx<'tx>,
{
  fn get(&self, key: &[u8]) -> Option<ValueDelta> {
    todo!()
  }
}

/*
 So now we are at the point of handling mutable transactions

*/
