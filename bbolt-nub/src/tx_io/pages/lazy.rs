use crate::tx_io::TxSlot;
use crate::tx_io::backends::ReadLazyIO;
use crate::tx_io::bytes::TxBytes;
use crate::tx_io::pages::ReadLazyPageIO;

#[derive(Clone)]
pub struct LazyPage<'tx, R: ReadLazyPageIO<'tx>> {
  tx: TxSlot<'tx>,
  root: R::PageBytes,
  r: R,
}
