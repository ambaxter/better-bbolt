use parking_lot::RwLockReadGuard;
use crate::common::errors::DiskReadError;
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId};
use crate::io::backends::{IOPageReader, IOReader, IOOverflowPageReader};
use crate::io::bytes::{FromIOBytes, IOBytes, IntoTxBytes, TxBytes};
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::bytes::shared_bytes::{SharedBytes, SharedTxBytes};
use crate::io::TxSlot;


pub struct TxHandle<'tx, IO> {
  io: RwLockReadGuard<'tx, IO>,
}


pub trait TxPageReader<'tx, IO: IOPageReader> {
  type Bytes: TxBytes<'tx> + FromIOBytes<'tx, IO::Bytes>;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError>;
}


impl<'tx, IO> TxPageReader<'tx, IO> for TxHandle<'tx, IO> where IO: IOPageReader<Bytes=SharedBytes> {
  type Bytes = SharedTxBytes<'tx>;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_meta_page(meta_page_id).map(|bytes| bytes.into_tx())
  }

  fn read_freelist_page(&self, freelist_page_id: FreelistPageId) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_freelist_page(freelist_page_id).map(|bytes| bytes.into_tx())
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_node_page(node_page_id).map(|bytes| bytes.into_tx())
  }
}


impl<'tx, IO> TxPageReader<'tx, IO> for TxHandle<'tx, IO> where IO: IOPageReader<Bytes=RefBytes> {
  type Bytes = &'tx [u8];

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_meta_page(meta_page_id).map(|bytes| bytes.into_tx())
  }

  fn read_freelist_page(&self, freelist_page_id: FreelistPageId) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_freelist_page(freelist_page_id).map(|bytes| bytes.into_tx())
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_node_page(node_page_id).map(|bytes| bytes.into_tx())
  }
}

pub trait TxOverflowPageReader<'tx, IO: IOPageReader>: TxPageReader<'tx, IO> {
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;
}


impl<'tx, IO> TxOverflowPageReader<'tx, IO> for TxHandle<'tx, IO> where IO: IOOverflowPageReader<Bytes=SharedBytes>
{
  fn read_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_freelist_page(freelist_page_id).map(|bytes| bytes.into_tx())
  }

  fn read_node_overflow(&self, node_page_id: NodePageId, overflow: u32) -> error_stack::Result<Self::Bytes, DiskReadError> {
    self.io.read_node_overflow(node_page_id, overflow).map(|bytes| bytes.into_tx())
  }
}

