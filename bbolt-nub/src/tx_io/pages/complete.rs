use crate::tx_io::TxSlot;
use crate::tx_io::bytes::TxBytes;

#[derive(Clone)]
pub struct CompletePage<'tx, P: TxBytes<'tx>> {
  tx: TxSlot<'tx>,
  root: P,
}
