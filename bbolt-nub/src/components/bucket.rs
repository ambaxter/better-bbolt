use crate::components::tx::TheTx;
use crate::io::pages::types::node::NodePage;

pub trait BucketApi {}

pub struct CoreBucket<'tx, T: TheTx<'tx>> {
  pub(crate) tx: &'tx T,
  pub(crate) root: NodePage<'tx, T::TxPageType>,
}
