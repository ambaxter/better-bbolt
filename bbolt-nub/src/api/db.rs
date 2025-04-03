use crate::api::bytes::TxSlice;
use triomphe::Arc;

#[derive(Clone)]
pub struct DbStats {
  inner: Arc<DbStatsInner>,
}

struct DbStatsInner {}

pub trait DbApi: Sized {
  type SliceType<'tx>: TxSlice<'tx>;

  fn stats(&self) -> DbStats;

  fn writable(&self) -> bool;
}

pub trait MutDbApi: DbApi {}
