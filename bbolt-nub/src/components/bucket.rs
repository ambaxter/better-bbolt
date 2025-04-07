use crate::components::tx::TheTx;
use crate::io::pages::types::node::NodePage;

pub struct CoreBucket<'tx, T: TheTx<'tx>> {
  tx: &'tx T,
  root: NodePage<'tx, T::TxPageType>
}
