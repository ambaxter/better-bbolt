use crate::components::tx::TheTx;
use crate::io::pages::types::node::NodePage;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::iter::FusedIterator;
use std::sync::Arc;

pub trait BucketApi {}

pub struct CoreBucket<'a, B, L, T> {
  pub(crate) tx: &'a T,
  pub(crate) root: NodePage<B, L>,
}

pub enum ValueDelta {
  Upsert(Arc<[u8]>),
  Delete,
}

pub struct CoreMutBucket<'a, T> {
  pub(crate) tx: &'a T,
  pub(crate) delta: BTreeMap<Arc<[u8]>, ValueDelta>,
}

/*
 So now we are at the point of handling mutable transactions

*/
