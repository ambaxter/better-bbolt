use crate::api::tx::TxStats;
use crate::common::errors::{DiskReadError, PageError};
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId, TxId};
use crate::common::layout::meta::Meta;
use crate::io::TxSlot;
use crate::io::backends::{IOOverflowPageReader, IOPageReader, IOReader};
use crate::io::bytes::ref_bytes::{RefBytes, RefTxBytes};
use crate::io::bytes::shared_bytes::{SharedBytes, SharedTxBytes};
use crate::io::bytes::{FromIOBytes, IOBytes, IntoTxBytes, TxBytes};
use crate::io::pages::direct::DirectPage;
use crate::io::pages::lazy::LazyPage;
use crate::io::pages::types::freelist::FreelistPage;
use crate::io::pages::types::meta::MetaPage;
use crate::io::pages::types::node::NodePage;
use crate::io::pages::{TxPage, TxPageType, TxReadLazyPageIO, TxReadPageIO};
use error_stack::ResultExt;
use parking_lot::RwLockReadGuard;
use triomphe::Arc;

pub trait TheTx<'tx>: TxReadPageIO<'tx> {
  fn stats(&self) -> &TxStats;
}

pub trait TheLazyTx<'tx>: TheTx<'tx> + TxReadLazyPageIO<'tx> {}

pub struct CoreTxHandle<'tx, IO> {
  pub(crate) io: RwLockReadGuard<'tx, IO>,
  pub(crate) stats: Arc<TxStats>,
  pub(crate) tx_id: TxId,
}

pub struct SharedTxHandle<'tx, IO> {
  pub(crate) handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for SharedTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  type TxPageType = DirectPage<'tx, SharedTxBytes<'tx>>;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidMeta(meta_page_id))?;
    MetaPage::try_from(TxPage::new(page)).change_context(PageError::InvalidMeta(meta_page_id))
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidFreelist(freelist_page_id))?;
    FreelistPage::try_from(TxPage::new(page))
      .change_context(PageError::InvalidFreelist(freelist_page_id))
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidNode(node_page_id))?;
    NodePage::try_from(TxPage::new(page)).change_context(PageError::InvalidNode(node_page_id))
  }
}

impl<'tx, IO> TheTx<'tx> for SharedTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  #[inline]
  fn stats(&self) -> &TxStats {
    &*self.handle.stats
  }
}

pub struct RefTxHandle<'tx, IO> {
  handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for RefTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, RefTxBytes<'tx>>,
{
  type TxPageType = DirectPage<'tx, RefTxBytes<'tx>>;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidMeta(meta_page_id))?;
    MetaPage::try_from(TxPage::new(page)).change_context(PageError::InvalidMeta(meta_page_id))
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidFreelist(freelist_page_id))?;
    FreelistPage::try_from(TxPage::new(page))
      .change_context(PageError::InvalidFreelist(freelist_page_id))
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidNode(node_page_id))?;
    NodePage::try_from(TxPage::new(page)).change_context(PageError::InvalidNode(node_page_id))
  }
}

impl<'tx, IO> TheTx<'tx> for RefTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, RefTxBytes<'tx>>,
{
  #[inline]
  fn stats(&self) -> &TxStats {
    &*self.handle.stats
  }
}

pub struct LazyTxHandle<'tx, IO> {
  pub(crate) handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for LazyTxHandle<'tx, IO>
where
  IO: IOOverflowPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  type TxPageType = LazyPage<'tx, Self>;

  fn read_meta_page(
    &'tx self, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<'tx, Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| LazyPage::new(bytes.into_tx(), self))
      .change_context(PageError::InvalidMeta(meta_page_id))?;
    MetaPage::try_from(TxPage::new(page)).change_context(PageError::InvalidMeta(meta_page_id))
  }

  fn read_freelist_page(
    &'tx self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<'tx, Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .map(|bytes| LazyPage::new(bytes.into_tx(), self))
      .change_context(PageError::InvalidFreelist(freelist_page_id))?;
    FreelistPage::try_from(TxPage::new(page))
      .change_context(PageError::InvalidFreelist(freelist_page_id))
  }

  fn read_node_page(
    &'tx self, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<'tx, Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_node_page(node_page_id)
      .map(|bytes| LazyPage::new(bytes.into_tx(), self))
      .change_context(PageError::InvalidNode(node_page_id))?;
    NodePage::try_from(TxPage::new(page)).change_context(PageError::InvalidNode(node_page_id))
  }
}

impl<'tx, IO> TxReadLazyPageIO<'tx> for LazyTxHandle<'tx, IO>
where
  IO: IOOverflowPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError> {
    self
      .handle
      .io
      .read_freelist_overflow(freelist_page_id, overflow)
      .map(|bytes| bytes.into_tx())
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError> {
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
{
  #[inline]
  fn stats(&self) -> &TxStats {
    &*self.handle.stats
  }
}

impl<'tx, IO> TheLazyTx<'tx> for LazyTxHandle<'tx, IO>
where
  IO: IOOverflowPageReader,
  IO::Bytes: IntoTxBytes<'tx, SharedTxBytes<'tx>>,
{
}
