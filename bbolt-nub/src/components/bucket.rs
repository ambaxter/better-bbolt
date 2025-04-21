use crate::components::tx::TheTx;
use crate::io::pages::types::node::NodePage;

pub trait BucketApi {}

pub struct CoreBucket<'tx, B, L, T> {
  pub(crate) tx: &'tx T,
  pub(crate) root: NodePage<B, L>,
}
