use crate::tx_io::TxSlot;
use crate::tx_io::bytes::TxBytes;
use crate::tx_io::pages::{GetKvRefSlice, GetKvTxSlice, Page, ReadPageIO, TxPage};
use std::ops::RangeBounds;

#[derive(Clone)]
pub struct EntirePage<'tx, P: TxBytes<'tx>> {
  tx: TxSlot<'tx>,
  root: P,
}

impl<'tx, P: TxBytes<'tx>> Page for EntirePage<'tx, P> {
  fn root_page(&self) -> &[u8] {
    &self.root
  }
}

impl<'tx, P: TxBytes<'tx>> GetKvRefSlice for EntirePage<'tx, P>
where
  P: GetKvRefSlice,
{
  type RefKv<'a>
    = P::RefKv<'a>
  where
    Self: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    self.root.get_ref_slice(range)
  }
}

impl<'tx, P: TxBytes<'tx>> GetKvTxSlice<'tx> for EntirePage<'tx, P>
where
  P: GetKvTxSlice<'tx>,
{
  type TxKv = P::TxKv;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    self.root.get_tx_slice(range)
  }
}

impl<'tx, P: TxBytes<'tx>> TxPage<'tx> for EntirePage<'tx, P> where P: GetKvTxSlice<'tx> {}
