use crate::common::data_pool::{DataPool, SharedData};
use crate::common::errors::{BucketError, CursorError, OpsError};
use crate::common::layout::bucket::BucketHeader;
use crate::common::layout::node::LeafFlag;
use crate::common::vec_pool::{UniqueVec, VecPool};
use crate::components::cursor::{
  CoreCursor, CoreCursorApi, CoreCursorSeekApi, CoreCursorTrySeekApi, LeafFlagFilterCursor,
  StackEntry,
};
use crate::components::tx::{TheMutTx, TheTx};
use crate::io::bytes::ref_bytes::RefTryBuf;
use crate::io::pages::direct::ops::{DirectGet, KvDataType, KvEq, KvOrd};
use crate::io::pages::lazy::ops::{
  KvTryDataType, KvTryEq, KvTryOrd, LazyRefIntoTryBuf, RefIntoTryBuf, TryBuf, TryEq, TryGet,
  TryHash, TryPartialEq, TryPartialOrd,
};
use crate::io::pages::lazy::{
  try_partial_cmp_buf_lazy_buf, try_partial_cmp_lazy_buf_buf, try_partial_cmp_lazy_buf_lazy_buf,
  try_partial_eq_lazy_buf_buf, try_partial_eq_lazy_buf_lazy_buf,
};
use crate::io::pages::types::node::{HasKeys, NodePage};
use crate::io::pages::{GatKvRef, GetKvTxSlice, TxPageType};
use error_stack::ResultExt;
use parking_lot::{Mutex, MutexGuard};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;
use std::ops::{Deref, Range};
use std::sync;

pub struct OnDiskBucket<B, L, TX> {
  pub(crate) tx: sync::Arc<TX>,
  pub(crate) stack_pool: VecPool<StackEntry<B, L>>,
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
    let stack = self.stack_pool.pop();
    let core_cursor = CoreCursor::new_with_stack(self, stack);
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
  ) -> crate::Result<Option<<TX::LeafType as HasKeys<'tx>>::TxKv>, BucketError> {
    let stack = self.stack_pool.pop();
    let core_cursor = CoreCursor::new_with_stack(self, stack);
    let mut c = LeafFlagFilterCursor::new(core_cursor, LeafFlag::default());
    match c.try_seek(key) {
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

#[derive(Clone)]
pub enum DeltaKv<O: AsRef<[u8]>> {
  OnDisk(O),
  Delta(SharedData),
}

impl<O: AsRef<[u8]>> AsRef<[u8]> for DeltaKv<O> {
  fn as_ref(&self) -> &[u8] {
    match self {
      DeltaKv::OnDisk(d) => d.as_ref(),
      DeltaKv::Delta(u) => u.as_ref(),
    }
  }
}

impl<O: AsRef<[u8]>> Deref for DeltaKv<O> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.as_ref()
  }
}

impl<O: AsRef<[u8]>> PartialEq for DeltaKv<O> {
  fn eq(&self, other: &Self) -> bool {
    self.as_ref() == other.as_ref()
  }
}

impl<O: AsRef<[u8]>> Eq for DeltaKv<O> {}

impl<O: AsRef<[u8]>> Ord for DeltaKv<O> {
  fn cmp(&self, other: &Self) -> Ordering {
    self.as_ref().cmp(other.as_ref())
  }
}

impl<O: AsRef<[u8]>> PartialOrd for DeltaKv<O> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    self.as_ref().partial_cmp(other.as_ref())
  }
}

impl<O: AsRef<[u8]>> DirectGet<u8> for DeltaKv<O> {
  fn direct_get(&self, index: usize) -> Option<u8> {
    self.as_ref().direct_get(index)
  }
}

impl<O: AsRef<[u8]>> Hash for DeltaKv<O> {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.as_ref().hash(state)
  }
}

impl<O: AsRef<[u8]>> PartialEq<[u8]> for DeltaKv<O> {
  fn eq(&self, other: &[u8]) -> bool {
    self.as_ref().eq(other)
  }
}

impl<O: AsRef<[u8]>> PartialOrd<[u8]> for DeltaKv<O> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    self.as_ref().partial_cmp(other)
  }
}

impl<O: AsRef<[u8]>> KvEq for DeltaKv<O> {}

impl<O: AsRef<[u8]>> KvOrd for DeltaKv<O> {}

impl<O: AsRef<[u8]>> KvDataType for DeltaKv<O> {}

#[derive(Clone)]
pub enum TryDeltaKv<O: KvTryDataType> {
  OnDisk(O),
  Delta(SharedData),
}

pub enum TryDeltaKvTryBuf<'a, O: TryBuf> {
  OnDisk(O),
  Delta(RefTryBuf<'a>),
}

impl<O: KvTryDataType> RefIntoTryBuf for TryDeltaKv<O> {
  type TryBuf<'a>
    = TryDeltaKvTryBuf<'a, O::TryBuf<'a>>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error> {
    match self {
      TryDeltaKv::OnDisk(o) => o
        .ref_into_try_buf()
        .map(TryDeltaKvTryBuf::OnDisk)
        .change_context(OpsError::TryBuf),
      TryDeltaKv::Delta(d) => d.ref_into_try_buf().map(TryDeltaKvTryBuf::Delta),
    }
  }
}

impl<'a, O> TryBuf for TryDeltaKvTryBuf<'a, O>
where
  O: TryBuf,
{
  type Error = OpsError;

  fn remaining(&self) -> usize {
    match self {
      TryDeltaKvTryBuf::OnDisk(o) => o.remaining(),
      TryDeltaKvTryBuf::Delta(d) => d.remaining(),
    }
  }

  fn chunk(&self) -> &[u8] {
    match self {
      TryDeltaKvTryBuf::OnDisk(o) => o.chunk(),
      TryDeltaKvTryBuf::Delta(d) => d.chunk(),
    }
  }

  fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
    match self {
      TryDeltaKvTryBuf::OnDisk(o) => o.try_advance(cnt).change_context(OpsError::TryBuf),
      TryDeltaKvTryBuf::Delta(d) => d.try_advance(cnt),
    }
  }
}

impl<'a, O: KvTryDataType> LazyRefIntoTryBuf for TryDeltaKv<O> {}

impl<O: KvTryDataType> TryPartialEq<[u8]> for TryDeltaKv<O> {
  type Error = OpsError;

  fn try_eq(&self, other: &[u8]) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_buf(self, other)
  }
}

impl<O: KvTryDataType> TryPartialEq for TryDeltaKv<O> {
  type Error = OpsError;

  fn try_eq(&self, other: &Self) -> crate::Result<bool, Self::Error> {
    try_partial_eq_lazy_buf_lazy_buf(self, other)
  }
}

impl<O: KvTryDataType> TryEq for TryDeltaKv<O> {}

impl<O: KvTryDataType> KvTryEq for TryDeltaKv<O> {}

impl<O: KvTryDataType> TryPartialOrd<[u8]> for TryDeltaKv<O> {
  fn try_partial_cmp(&self, other: &[u8]) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_buf(self, other)
  }
}

impl<O: KvTryDataType> TryPartialOrd for TryDeltaKv<O> {
  fn try_partial_cmp(&self, other: &Self) -> crate::Result<Option<Ordering>, Self::Error> {
    try_partial_cmp_lazy_buf_lazy_buf(self, other)
  }
}

impl<O: KvTryDataType> KvTryOrd for TryDeltaKv<O> {}

impl<O: KvTryDataType> TryHash for TryDeltaKv<O> {
  type Error = BucketError;

  fn try_hash<H: Hasher>(&self, state: &mut H) -> crate::Result<(), Self::Error> {
    match self {
      TryDeltaKv::OnDisk(o) => o
        .try_hash(state)
        .change_context(BucketError::TruBuffKvError),
      TryDeltaKv::Delta(d) => Ok(d.hash(state)),
    }
  }
}

impl<O: KvTryDataType> TryGet<u8> for TryDeltaKv<O> {
  type Error = BucketError;

  fn try_get(&self, index: usize) -> crate::Result<Option<u8>, Self::Error> {
    match self {
      TryDeltaKv::OnDisk(o) => o.try_get(index).change_context(BucketError::TruBuffKvError),
      TryDeltaKv::Delta(d) => d.try_get(index).change_context(BucketError::TruBuffKvError),
    }
  }
}

impl<O: KvTryDataType> KvTryDataType for TryDeltaKv<O> {}

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
