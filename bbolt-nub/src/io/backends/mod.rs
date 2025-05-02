use crate::common::errors::DiskError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::bytes::IOBytes;
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::bytes::shared_bytes::SharedBytes;
use crate::io::transmogrify::{TxContext, TxDirectContext};
use bytes::BufMut;
use delegate::delegate;
use error_stack::Report;
use fs_err::File;
use moka::Entry;
use moka::ops::compute::Op;
use moka::sync::Cache;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

pub mod channel_store;

pub mod file;

#[cfg(target_family = "unix")]
pub mod p_file;

#[cfg(target_os = "linux")]
#[cfg(feature = "io_uring")]
pub mod io_uring;

pub mod memmap;
pub mod meta_reader;

pub mod trait_playground;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IOType {
  RO,
  WO,
  RW,
}

pub struct IOCore {
  path: Arc<PathBuf>,
  page_size: usize,
  io_type: IOType,
}

pub trait IOBackend: Sized {
  type ConfigOptions: Clone + Sized;

  fn io_type(&self) -> IOType;

  fn page_size(&self) -> usize;

  fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), DiskError>;
}

pub trait WriteablePage {
  fn header(&self) -> &PageHeader;
  fn write_out<B: BufMut>(self, mut_buf: &mut B) -> crate::Result<(), DiskError>;
}

pub trait BackendableBackendWritePageDamnit: BufMut {
  fn write(self) -> crate::Result<usize, DiskError>;
}

pub trait IOWriter {
  fn page_size(&self) -> usize;
  fn write_single_page(
    &self, disk_page_id: DiskPageId, page: SharedBytes,
  ) -> crate::Result<(), DiskError>;
}

pub trait IOReader {
  type Bytes: IOBytes;

  fn page_size(&self) -> usize;

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError>;

  fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, DiskError> {
    let page_size = self.page_size();
    let page_len = page_size;
    self.read_disk_page(disk_page_id, page_len)
  }
}

pub trait ContigIOReader: IOReader {
  fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, DiskError>;
  fn read_contig_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, DiskError> {
    let page_size = self.page_size();
    let header = self.read_header(disk_page_id)?;
    let overflow = header.get_overflow();
    let page_len = page_size + (overflow + 1) as usize;
    self.read_disk_page(disk_page_id, page_len)
  }
}

pub struct ROShell<R> {
  read: R,
}

impl<R> ROShell<R> {
  pub fn new(read: R) -> Self {
    Self { read }
  }
}

impl<R> IOReader for ROShell<R>
where
  R: IOReader,
{
  type Bytes = R::Bytes;

  delegate! {
    to self.read {
      fn page_size(&self) -> usize;
      fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError>;
      fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, DiskError>;
    }
  }
}

pub struct WOShell<W> {
  write: W,
}

impl<W> WOShell<W> {
  pub fn new(write: W) -> Self {
    Self { write }
  }
}

pub struct RWShell<R, W> {
  read: ROShell<R>,
  write: WOShell<W>,
}

impl<R, W> RWShell<R, W> {
  pub fn new(read: R, write: W) -> Self {
    Self { read, write }
  }
}

impl<R, W> IOReader for RWShell<R, W>
where
  R: IOReader,
{
  type Bytes = R::Bytes;

  delegate! {
    to self.read {
      fn page_size(&self) -> usize;
      fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError>;
      fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, DiskError>;
    }
  }
}

pub struct DirectReadHandler<T, I> {
  pub(crate) tx_context: T,
  pub(crate) io: I,
}

pub trait IOPageReader {
  type Bytes: IOBytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskError>;

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskError>;

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskError>;
}

impl<T, I> IOPageReader for DirectReadHandler<T, I>
where
  T: TxDirectContext,
  I: ContigIOReader,
{
  type Bytes = I::Bytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self.tx_context.trans_meta_id(meta_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self.tx_context.trans_freelist_id(freelist_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self.tx_context.trans_node_id(node_page_id);
    self.io.read_contig_page(disk_page_id)
  }
}

pub trait ReadLoadedPageIO: IOPageReader {}

impl<T, I> ReadLoadedPageIO for DirectReadHandler<T, I>
where
  T: TxDirectContext,
  I: ContigIOReader,
{
}

pub trait IOOverflowPageReader: IOPageReader {
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskError>;

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskError>;
}

pub struct CachedReadHandler<T, I: IOReader<Bytes = SharedBytes>> {
  pub(crate) handler: DirectReadHandler<T, I>,
  pub(crate) page_cache: Cache<DiskPageId, SharedBytes>,
}

impl<T, I> CachedReadHandler<T, I>
where
  T: TxContext,
  I: IOReader<Bytes = SharedBytes>,
{
  fn read_cache_or_disk(&self, disk_page_id: DiskPageId) -> crate::Result<SharedBytes, DiskError> {
    self
      .page_cache
      .entry(disk_page_id)
      .and_try_compute_with(|entry| match entry {
        None => self
          .handler
          .io
          .read_single_page(disk_page_id)
          .map(|bytes| Op::Put(bytes)),
        Some(_) => Ok(Op::Nop),
      })
      .map(|comp_result| {
        comp_result
          .into_entry()
          .expect("Should not be StillNone")
          .value()
          .clone()
      })
  }
}

impl<T, I> IOPageReader for CachedReadHandler<T, I>
where
  T: TxContext,
  I: IOReader<Bytes = SharedBytes>,
{
  type Bytes = SharedBytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self.handler.tx_context.trans_meta_id(meta_page_id);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self.handler.tx_context.trans_freelist_id(freelist_page_id);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self.handler.tx_context.trans_node_id(node_page_id);
    self.read_cache_or_disk(disk_page_id)
  }
}

impl<T, I> IOOverflowPageReader for CachedReadHandler<T, I>
where
  T: TxContext,
  I: IOReader<Bytes = SharedBytes>,
{
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_freelist_id(freelist_page_id + overflow);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_node_id(node_page_id + overflow);
    self.read_cache_or_disk(disk_page_id)
  }
}

pub struct RHandler<R> {
  path: PathBuf,
  lock: File,
  reader: R,
}

impl<R> IOPageReader for RHandler<R>
where
  R: IOPageReader,
{
  type Bytes = R::Bytes;

  delegate! {
      to self.reader {
        fn read_meta_page(&self, meta_page_id: MetaPageId)
      -> crate::Result<Self::Bytes, DiskError>;
        fn read_freelist_page(&self, freelist_page_id: FreelistPageId)
      -> crate::Result<Self::Bytes, DiskError>;
        fn read_node_page(&self, node_page_id: NodePageId)
      -> crate::Result<Self::Bytes, DiskError>;
    }
  }
}

impl<R> IOOverflowPageReader for RHandler<R>
where
  R: IOOverflowPageReader,
{
  delegate! {
    to self.reader {
        fn read_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32,)
      -> crate::Result<Self::Bytes, DiskError>;
        fn read_node_overflow(&self, node_page_id: NodePageId, overflow: u32,)
      -> crate::Result<Self::Bytes, DiskError>;
    }
  }
}

impl<R> RHandler<R> {}

pub struct RWHandler<R, W> {
  reader: RHandler<R>,
  w: W,
}

impl<R, W> RWHandler<R, W> {
  pub fn flush(&mut self) -> crate::Result<(), io::Error> {
    self.reader.lock.flush()?;
    Ok(())
  }
}

impl<R, W> IOPageReader for RWHandler<R, W>
where
  R: IOPageReader,
{
  type Bytes = R::Bytes;

  delegate! {
      to self.reader {
        fn read_meta_page(&self, meta_page_id: MetaPageId)
      -> crate::Result<Self::Bytes, DiskError>;
        fn read_freelist_page(&self, freelist_page_id: FreelistPageId)
      -> crate::Result<Self::Bytes, DiskError>;
        fn read_node_page(&self, node_page_id: NodePageId)
      -> crate::Result<Self::Bytes, DiskError>;
    }
  }
}

impl<R, W> IOOverflowPageReader for RWHandler<R, W>
where
  R: IOOverflowPageReader,
{
  delegate! {
    to self.reader {
        fn read_freelist_overflow(&self, freelist_page_id: FreelistPageId, overflow: u32,)
      -> crate::Result<Self::Bytes, DiskError>;
        fn read_node_overflow(&self, node_page_id: NodePageId, overflow: u32,)
      -> crate::Result<Self::Bytes, DiskError>;
    }
  }
}
