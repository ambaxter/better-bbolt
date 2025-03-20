use crate::api::bytes::TxSlice;
use triomphe::Arc;

#[derive(Clone)]
pub struct DbStats {
  inner: Arc<DbStatsInner>,
}

struct DbStatsInner {}

pub trait BoltDb: Sized {
  type SliceType<'tx>: TxSlice<'tx>;

  fn stats(&self) -> DbStats;

  fn writable(&self) -> bool;
}

pub trait MutBoltDb: BoltDb {}
