use crate::io::TxSlot;
use crate::io::bytes::TxBytes;
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType, TxReadPageIO};
use std::ops::RangeBounds;

#[derive(Clone)]
pub struct LoadedPage<'tx, P: TxBytes<'tx>> {
  tx: TxSlot<'tx>,
  root: P,
}

impl<'tx, P: TxBytes<'tx>> Page for LoadedPage<'tx, P> {
  fn root_page(&self) -> &[u8] {
    &self.root
  }
}

impl<'tx, P: TxBytes<'tx>> GetKvRefSlice for LoadedPage<'tx, P>
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

impl<'tx, P: TxBytes<'tx>> GetKvTxSlice<'tx> for LoadedPage<'tx, P>
where
  P: GetKvTxSlice<'tx>,
{
  type TxKv = P::TxKv;

  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv {
    self.root.get_tx_slice(range)
  }
}

impl<'tx, P: TxBytes<'tx>> TxPageType<'tx> for LoadedPage<'tx, P> where P: GetKvTxSlice<'tx> {}
