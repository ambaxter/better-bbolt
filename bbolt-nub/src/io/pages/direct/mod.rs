use crate::io::TxSlot;
use crate::io::bytes::TxBytes;
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, TxPageType};
use std::ops::RangeBounds;

pub mod ops;

#[derive(Clone)]
pub struct DirectPage<'tx, P: TxBytes<'tx>> {
  tx: TxSlot<'tx>,
  root: P,
}

impl<'tx, P: TxBytes<'tx>> DirectPage<'tx, P> {
  pub fn new(root: P) -> Self {
    DirectPage {
      tx: Default::default(),
      root,
    }
  }
}

impl<'tx, P: TxBytes<'tx>> Page for DirectPage<'tx, P> {
  fn root_page(&self) -> &[u8] {
    &self.root
  }
}

impl<'a, 'tx, P: TxBytes<'tx>> GatKvRef<'a> for DirectPage<'tx, P>
where
  P: GetGatKvRefSlice,
{
  type KvRef = <P as GatKvRef<'a>>::KvRef;
}

impl<'tx, P: TxBytes<'tx>> GetGatKvRefSlice for DirectPage<'tx, P>
where
  P: GetGatKvRefSlice,
{
  #[inline]
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> <Self as GatKvRef<'a>>::KvRef {
    self.root.get_ref_slice(range)
  }
}

impl<'tx, P: TxBytes<'tx>> GetKvTxSlice<'tx> for DirectPage<'tx, P>
where
  P: GetKvTxSlice<'tx>,
{
  type KvTx = P::KvTx;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::KvTx {
    self.root.get_tx_slice(range)
  }
}

impl<'tx, P: TxBytes<'tx>> TxPageType<'tx> for DirectPage<'tx, P>
where
  P: GetKvTxSlice<'tx>,
{
  type TxPageBytes = P;
}
