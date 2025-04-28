use crate::api::tx::TxStats;
use crate::common::data_pool::{DataPool, SharedData};
use crate::common::errors::{DiskReadError, PageError, TxError};
use crate::common::id::{FreelistPageId, MetaPageId, NodePageId, TxId};
use crate::common::layout::meta::Meta;
use crate::components::bucket::{BucketDelta, OnDiskBucket, ValueDelta};
use crate::components::bucket_path::BucketPathBuf;
use crate::io::TxSlot;
use crate::io::backends::{IOOverflowPageReader, IOPageReader, IOReader};
use crate::io::bytes::ref_bytes::{RefBytes, RefTxBytes};
use crate::io::bytes::shared_bytes::{SharedBytes, SharedTxBytes};
use crate::io::bytes::{FromIOBytes, IOBytes, IntoTxBytes, TxBytes};
use crate::io::pages::direct::DirectPage;
use crate::io::pages::lazy::LazyPage;
use crate::io::pages::lazy::ops::RefIntoTryBuf;
use crate::io::pages::types::freelist::FreelistPage;
use crate::io::pages::types::meta::MetaPage;
use crate::io::pages::types::node::NodePage;
use crate::io::pages::types::node::branch::bbolt::BBoltBranch;
use crate::io::pages::types::node::leaf::bbolt::BBoltLeaf;
use crate::io::pages::{TxPage, TxPageType, TxReadLazyPageIO, TxReadPageIO};
use delegate::delegate;
use error_stack::{FutureExt, ResultExt};
use hashbrown::HashSet;
use parking_lot::{Mutex, RwLockReadGuard, RwLockUpgradableReadGuard};
use std::collections::BTreeMap;
use std::sync;

pub trait TheTx<'tx>: TxReadPageIO<'tx> {
  fn stats(&self) -> &TxStats;
}

pub trait TheMutTx<'tx>: TheTx<'tx> {
  fn clone_key(&self, bytes: &[u8]) -> SharedData;
  fn try_clone_key<T>(&self, bytes: &T) -> crate::Result<SharedData, TxError>
  where
    T: RefIntoTryBuf;
  fn clone_value(&self, bytes: &[u8]) -> SharedData;
}

pub trait TheLazyTx<'tx>: TheTx<'tx> + TxReadLazyPageIO<'tx> {}

pub enum IOLockGuard<'tx, IO> {
  R(RwLockReadGuard<'tx, IO>),
  U(RwLockUpgradableReadGuard<'tx, IO>),
}

impl<'tx, IO> From<RwLockReadGuard<'tx, IO>> for IOLockGuard<'tx, IO>
where
  IO: IOPageReader,
{
  fn from(value: RwLockReadGuard<'tx, IO>) -> Self {
    IOLockGuard::R(value)
  }
}

impl<'tx, IO> From<RwLockUpgradableReadGuard<'tx, IO>> for IOLockGuard<'tx, IO>
where
  IO: IOPageReader,
{
  fn from(value: RwLockUpgradableReadGuard<'tx, IO>) -> Self {
    IOLockGuard::U(value)
  }
}

impl<'tx, IO> IOPageReader for IOLockGuard<'tx, IO>
where
  IO: IOPageReader,
{
  type Bytes = IO::Bytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError> {
    match self {
      IOLockGuard::R(io) => io.read_meta_page(meta_page_id),
      IOLockGuard::U(io) => io.read_meta_page(meta_page_id),
    }
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    match self {
      IOLockGuard::R(io) => io.read_freelist_page(freelist_page_id),
      IOLockGuard::U(io) => io.read_freelist_page(freelist_page_id),
    }
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError> {
    match self {
      IOLockGuard::R(io) => io.read_node_page(node_page_id),
      IOLockGuard::U(io) => io.read_node_page(node_page_id),
    }
  }
}

impl<'tx, IO> IOOverflowPageReader for IOLockGuard<'tx, IO>
where
  IO: IOOverflowPageReader,
{
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    match self {
      IOLockGuard::R(io) => io.read_freelist_overflow(freelist_page_id, overflow),
      IOLockGuard::U(io) => io.read_freelist_overflow(freelist_page_id, overflow),
    }
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    match self {
      IOLockGuard::R(io) => io.read_node_overflow(node_page_id, overflow),
      IOLockGuard::U(io) => io.read_node_overflow(node_page_id, overflow),
    }
  }
}

pub struct CoreTxHandle<'tx, IO> {
  pub(crate) io: IOLockGuard<'tx, IO>,
  pub(crate) stats: sync::Arc<TxStats>,
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
  type BranchType = BBoltBranch<'tx, Self::TxPageType>;
  type LeafType = BBoltLeaf<'tx, Self::TxPageType>;

  fn read_meta_page(
    self: &sync::Arc<Self>, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<'tx, Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidMeta(meta_page_id))?;
    MetaPage::try_from(TxPage::new(page)).change_context(PageError::InvalidMeta(meta_page_id))
  }

  fn read_freelist_page(
    self: &sync::Arc<Self>, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<'tx, Self::TxPageType>, PageError> {
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
    self: &sync::Arc<Self>, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<Self::BranchType, Self::LeafType>, PageError> {
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
  pub(crate) handle: CoreTxHandle<'tx, IO>,
}

impl<'tx, IO> TxReadPageIO<'tx> for RefTxHandle<'tx, IO>
where
  IO: IOPageReader,
  IO::Bytes: IntoTxBytes<'tx, RefTxBytes<'tx>>,
{
  type TxPageType = DirectPage<'tx, RefTxBytes<'tx>>;
  type BranchType = BBoltBranch<'tx, Self::TxPageType>;
  type LeafType = BBoltLeaf<'tx, Self::TxPageType>;

  fn read_meta_page(
    self: &sync::Arc<Self>, meta_page_id: MetaPageId,
  ) -> crate::Result<MetaPage<'tx, Self::TxPageType>, PageError> {
    let page = self
      .handle
      .io
      .read_meta_page(meta_page_id)
      .map(|bytes| DirectPage::new(bytes.into_tx()))
      .change_context(PageError::InvalidMeta(meta_page_id))?;
    MetaPage::try_from(TxPage::new(page)).change_context(PageError::InvalidMeta(meta_page_id))
  }

  fn read_freelist_page(
    self: &sync::Arc<Self>, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<'tx, Self::TxPageType>, PageError> {
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
    self: &sync::Arc<Self>, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<Self::BranchType, Self::LeafType>, PageError> {
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
  type BranchType = BBoltBranch<'tx, Self::TxPageType>;
  type LeafType = BBoltLeaf<'tx, Self::TxPageType>;

  fn read_meta_page(
    self: &sync::Arc<Self>, meta_page_id: MetaPageId,
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
    self: &sync::Arc<Self>, freelist_page_id: FreelistPageId,
  ) -> crate::Result<FreelistPage<'tx, Self::TxPageType>, PageError> {
    let bytes = self
      .handle
      .io
      .read_freelist_page(freelist_page_id)
      .change_context(PageError::InvalidFreelist(freelist_page_id))?;
    let page = LazyPage::new(IntoTxBytes::<'tx>::into_tx(bytes), self);
    FreelistPage::try_from(TxPage::new(page))
      .change_context(PageError::InvalidFreelist(freelist_page_id))
  }

  fn read_node_page(
    self: &sync::Arc<Self>, node_page_id: NodePageId,
  ) -> crate::Result<NodePage<Self::BranchType, Self::LeafType>, PageError> {
    let bytes = self
      .handle
      .io
      .read_node_page(node_page_id)
      .change_context(PageError::InvalidNode(node_page_id))?;
    let page = LazyPage::new(IntoTxBytes::<'tx>::into_tx(bytes), self);
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

pub struct MutTxHandle<TX> {
  tx: sync::Arc<TX>,
  data_pool: DataPool,
  key_set: Mutex<HashSet<SharedData>>,
  delta_map: Mutex<BTreeMap<BucketPathBuf, BucketDelta>>,
}

impl<'tx, TX> TxReadPageIO<'tx> for MutTxHandle<TX>
where
  TX: TxReadPageIO<'tx>,
{
  type TxPageType = TX::TxPageType;
  type BranchType = TX::BranchType;
  type LeafType = TX::LeafType;

  delegate! {
      to self.tx {
      fn read_meta_page(self: &sync::Arc<Self>, meta_page_id: MetaPageId) -> crate::Result<MetaPage<'tx, Self::TxPageType>, PageError>;
      fn read_freelist_page(self: &sync::Arc<Self>, freelist_page_id: FreelistPageId) -> crate::Result<FreelistPage<'tx, Self::TxPageType>, PageError>;
      fn read_node_page(self: &sync::Arc<Self>, node_page_id: NodePageId) -> crate::Result<NodePage<Self::BranchType, Self::LeafType>, PageError>;
      }
  }
}

impl<'tx, TX> TheTx<'tx> for MutTxHandle<TX>
where
  TX: TheTx<'tx>,
{
  delegate! {
      to &self.tx {
          fn stats(&self) -> &TxStats;
      }
  }
}

impl<'tx, TX> TxReadLazyPageIO<'tx> for MutTxHandle<TX>
where
  TX: TxReadLazyPageIO<'tx>,
{
  delegate! {
      to self.tx {
      fn read_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32) -> crate::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError>;
      fn read_node_overflow(&self, node_page_id: NodePageId, overflow: u32) -> crate::Result<<Self::TxPageType as TxPageType<'tx>>::TxPageBytes, DiskReadError>;
      }
  }
}

impl<'tx, TX> TheLazyTx<'tx> for MutTxHandle<TX> where TX: TheLazyTx<'tx> {}

impl<'tx, TX> TheMutTx<'tx> for MutTxHandle<TX>
where
  TX: TheTx<'tx>,
{
  fn clone_key(&self, bytes: &[u8]) -> SharedData {
    let mut key_set = self.key_set.lock();
    key_set
      .get_or_insert_with(bytes, |f| {
        let mut unique = self.data_pool.pop();
        unique.copy_data_and_share(bytes)
      })
      .clone()
  }

  fn try_clone_key<T>(&self, bytes: &T) -> crate::Result<SharedData, TxError>
  where
    T: RefIntoTryBuf,
  {
    let data = bytes
      .ref_into_try_buf()
      .and_then(|try_buf| {
        let mut unique = self.data_pool.pop();
        unique.copy_try_buf_and_share(try_buf)
      })
      .change_context(TxError::DataCopy)?;
    let mut key_set = self.key_set.lock();
    let key = key_set.get_or_insert(data);
    Ok(key.clone())
  }

  fn clone_value(&self, bytes: &[u8]) -> SharedData {
    let mut unique = self.data_pool.pop();
    unique.copy_data_and_share(bytes)
  }
}

impl<'tx, TX> MutTxHandle<TX> where TX: TheTx<'tx> {}
