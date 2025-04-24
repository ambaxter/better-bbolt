use crate::components::tx::TheTx;
use crate::io::pages::types::node::NodePage;
use std::collections::BTreeMap;
use triomphe::Arc;

pub trait BucketApi {}

pub struct CoreBucket<'tx, B, L, T> {
  pub(crate) tx: &'tx T,
  pub(crate) root: NodePage<B, L>,
}

pub enum ValueDelta {
  Upsert(Arc<[u8]>),
  Delete,
}

pub struct CoreMutBucket<'tx, T> {
  pub(crate) tx: &'tx T,
  pub(crate) delta: BTreeMap<Arc<[u8]>, ValueDelta>,
}
