use crate::common::errors::DiskReadError;
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId, TxId};
use crate::common::layout::meta::Meta;
use crate::io::TxSlot;
use crate::io::backends::{IOOverflowPageReader, IOPageReader, IOReader};
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::bytes::shared_bytes::{SharedBytes, SharedTxBytes};
use crate::io::bytes::{FromIOBytes, IOBytes, IntoTxBytes, TxBytes};
use crate::io::pages::lazy::LazyPage;
use crate::io::pages::loaded::LoadedPage;
use crate::io::pages::{TxPageType, TxReadLazyPageIO, TxReadPageIO};
use parking_lot::RwLockReadGuard;

pub trait TheTx<'tx> : TxReadPageIO<'tx> {}

pub struct CoreTxHandle<'tx, IO> {
  io: RwLockReadGuard<'tx, IO>,
  tx_id: TxId,
}

pub struct SharedTxHandle<'tx, IO> {
  handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for SharedTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  type TxPageType = LoadedPage<'tx, SharedTxBytes<'tx>>;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| LoadedPage::new(bytes.into_tx()))
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| LoadedPage::new(bytes.into_tx()))
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| LoadedPage::new(bytes.into_tx()))
  }
}

impl<'tx, IO> TheTx<'tx> for SharedTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{}

pub struct RefTxHandle<'tx, IO> {
  handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for RefTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, &'tx [u8]>,
{
  type TxPageType = LoadedPage<'tx, &'tx [u8]>;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| LoadedPage::new(bytes.into_tx()))
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| LoadedPage::new(bytes.into_tx()))
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> crate::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| LoadedPage::new(bytes.into_tx()))
  }
}

impl<'tx, IO> TheTx<'tx> for RefTxHandle<'tx, IO>
where
IO: IOPageReader,
IO::Bytes: IntoTxBytes<'tx, &'tx [u8]>,
{}

pub struct LazyTxHandle<'tx, IO> {
  handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for LazyTxHandle<'tx, IO>
where
  IO: IOOverflowPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  type TxPageType = LazyPage<'tx, Self>;

  fn read_meta_page(
    &'tx self, meta_page_id: MetaPageId,
  ) -> error_stack::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| LazyPage::new(bytes.into_tx(), self))
  }

  fn read_freelist_page(
    &'tx self, freelist_page_id: FreelistPageId,
  ) -> error_stack::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| LazyPage::new(bytes.into_tx(), self))
  }

  fn read_node_page(
    &'tx self, node_page_id: NodePageId,
  ) -> error_stack::Result<Self::TxPageType, DiskReadError> {
    self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| LazyPage::new(bytes.into_tx(), self))
  }
}

impl<'tx, IO> TxReadLazyPageIO<'tx> for LazyTxHandle<'tx, IO>
where
  IO: IOOverflowPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> error_stack::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_overflow(freelist_page_id, overflow)
      .map(|bytes| bytes.into_tx())
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> error_stack::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_node_overflow(node_page_id, overflow)
      .map(|bytes| bytes.into_tx())
  }
}


impl<'tx, IO> TheTx<'tx> for LazyTxHandle<'tx, IO>
where
  IO: IOOverflowPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{}