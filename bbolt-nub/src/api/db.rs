use crate::api::bytes::TxSlice;
use std::sync;

#[derive(Clone)]
pub struct DbStats {
  inner: sync::Arc<InnerDbStats>,
}

struct InnerDbStats {}

pub trait DbApi: Sized {
  type SliceType<'tx>: TxSlice<'tx>;

  fn stats(&self) -> DbStats;

  fn writable(&self) -> bool;
}

pub trait MutDbApi: DbApi {}
