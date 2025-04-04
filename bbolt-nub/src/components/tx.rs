use crate::common::errors::DiskReadError;
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId, TxId};
use crate::common::layout::meta::Meta;
use crate::io::TxSlot;
use crate::io::backends::{IOOverflowPageReader, IOPageReader, IOReader};
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::bytes::shared_bytes::{SharedBytes, SharedTxBytes};
use crate::io::bytes::{FromIOBytes, IOBytes, IntoTxBytes, TxBytes};
use crate::io::pages::TxReadPageIO;
use parking_lot::RwLockReadGuard;

pub struct InnerTxHandle<'tx, IO> {
  io: RwLockReadGuard<'tx, IO>,
  tx_id: TxId
}

pub struct SharedTxHandle<'tx, IO> {
  handle: InnerTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for SharedTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  type TxPageBytes = SharedTxBytes<'tx>;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<Self::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| bytes.into_tx())
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> error_stack::Result<Self::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| bytes.into_tx())
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> error_stack::Result<Self::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| bytes.into_tx())
  }
}

pub struct RefTxHandle<'tx, IO> {
  handle: InnerTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for RefTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, &'tx [u8]>,
{
  type TxPageBytes = &'tx [u8];

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<Self::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| bytes.into_tx())
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> error_stack::Result<Self::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| bytes.into_tx())
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> error_stack::Result<Self::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| bytes.into_tx())
  }
}
