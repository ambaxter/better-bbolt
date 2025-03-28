use crate::tx_io::TxSlot;
use crate::tx_io::bytes::TxBytes;
use crate::tx_io::pages::Page;

#[derive(Clone)]
pub struct CompletePage<'tx, P: TxBytes<'tx>> {
  tx: TxSlot<'tx>,
  root: P,
}

impl<'tx, P: TxBytes<'tx>> Page for CompletePage<'tx, P> {
  fn root_page(&self) -> &[u8] {
    &self.root
  }
}
