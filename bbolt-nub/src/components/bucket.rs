use crate::common::data_pool::SharedData;
use crate::common::errors::BucketError;
use crate::common::layout::bucket::BucketHeader;
use crate::components::cursor::{CoreCursor, CoreCursorSeekApi, LeafFlagFilterCursor, StackEntry};
use crate::components::tx::{TheMutTx, TheTx};
use crate::io::pages::types::node::NodePage;
use crate::io::pages::{GatKvRef, GetKvTxSlice, TxPageType};
use parking_lot::{Mutex, MutexGuard};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;
use std::ops::Deref;
use std::sync;

pub struct OnDiskBucket<'a, B, L, TX> {
  pub(crate) tx: &'a TX,
  pub(crate) header: BucketHeader,
  pub(crate) root: NodePage<B, L>,
}

impl<'p, B, L, TX> OnDiskBucket<'p, B, L, TX> {
  pub fn sequence(&self) -> u64 {
    self.header.sequence()
  }
}

impl<'p, 'tx, TX> OnDiskBucket<'p, TX::BranchType, TX::LeafType, TX>
where
  TX: TheTx<'tx>,
  for<'b> <TX::BranchType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
  for<'b> <TX::LeafType as GatKvRef<'b>>::KvRef: PartialOrd<[u8]>,
{
  fn get(
    &self, key: &[u8],
  ) -> crate::Result<<TX::TxPageType as GetKvTxSlice<'tx>>::KvTx, BucketError> {
    todo!()
  }
}

pub enum ValueDelta {
  UValue(SharedData),
  UBucket(SharedData),
  Delete,
}

pub enum BucketType<'a, B, L, T> {
  OnDisk(OnDiskBucket<'a, B, L, T>),
  UpsertOnly(&'a T),
}

impl<'a, B, L, T> BucketType<'a, B, L, T> {
  fn tx(&self) -> &T {
    match self {
      BucketType::OnDisk(bucket) => bucket.tx,
      BucketType::UpsertOnly(tx) => tx,
    }
  }
}

#[derive(Clone)]
pub struct BucketDelta {
  delta: sync::Arc<Mutex<BTreeMap<SharedData, ValueDelta>>>,
}

pub struct UpsertBucket<'a, B, L, T> {
  pub(crate) bucket_type: BucketType<'a, B, L, T>,
  pub(crate) delta: BucketDelta,
}

pub enum UpsertKv<D> {
  OnDisk(D),
  Upsert(SharedData),
}

impl<D> AsRef<[u8]> for UpsertKv<D>
where
  D: AsRef<[u8]>,
{
  fn as_ref(&self) -> &[u8] {
    match self {
      UpsertKv::OnDisk(d) => d.as_ref(),
      UpsertKv::Upsert(u) => u.as_ref(),
    }
  }
}

impl<D> Deref for UpsertKv<D>
where
  D: AsRef<[u8]>,
{
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl<'a, 'tx, B, L, TX> UpsertBucket<'a, B, L, TX>
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
