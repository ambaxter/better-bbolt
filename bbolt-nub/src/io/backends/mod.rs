use crate::common::errors::IOError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::bytes::IOBytes;
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::bytes::shared_bytes::SharedBytes;
use crate::io::transmogrify::{TxContext, TxDirectContext};
use bytes::BufMut;
use delegate::delegate;
use error_stack::{Report, ResultExt};
use moka::Entry;
use moka::ops::compute::Op;
use moka::sync::Cache;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
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

impl IOCore {
  pub fn new(path: Arc<PathBuf>, page_size: usize, io_type: IOType) -> Self {
    IOCore {
      path,
      page_size,
      io_type,
    }
  }

  #[inline]
  pub fn path(&self) -> &Path {
    self.path.as_ref()
  }

  pub fn open_file(&self) -> crate::Result<File, IOError> {
    let mut options = OpenOptions::new();
    match self.io_type {
      IOType::RO => options.read(true),
      IOType::WO => options.write(true),
      IOType::RW => options.read(true).write(true),
    };
    options
      .open(self.path())
      .change_context_lazy(|| IOError::OpenError(self.path().into()))
  }
}

pub trait IOBackend: Sized {
  fn io_type(&self) -> IOType;

  fn page_size(&self) -> usize;

  #[inline]
  fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), IOError> {
    Ok(())
  }
}

pub trait WriteablePage {
  fn header(&self) -> &PageHeader;
  fn write_out<B: BufMut>(self, mut_buf: &mut B) -> crate::Result<(), IOError>;
}

pub trait BackendableBackendWritePageDamnit: BufMut {
  fn write(self) -> crate::Result<usize, IOError>;
}

pub trait IOReader: IOBackend {
  type Bytes: IOBytes;

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, IOError>;

  fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, IOError> {
    let page_size = self.page_size();
    let page_len = page_size;
    self.read_disk_page(disk_page_id, page_len)
  }
}

pub trait NewIOReader: IOReader {
  type ReadOptions: Clone + Sized;

  fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: Self::ReadOptions,
  ) -> crate::Result<ROShell<Self>, IOError>;
}

pub trait ContigIOReader: IOReader {
  fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, IOError>;
  fn read_contig_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, IOError> {
    let page_size = self.page_size();
    let header = self.read_header(disk_page_id)?;
    let overflow = header.get_overflow();
    let page_len = page_size + (overflow + 1) as usize;
    self.read_disk_page(disk_page_id, page_len)
  }
}

pub trait IOWriter: IOBackend {
  fn write_single_page(
    &self, disk_page_id: DiskPageId, page: SharedBytes,
  ) -> crate::Result<(), IOError>;
}

pub trait NewIOWriter: IOWriter {
  type WriteOptions: Clone + Sized;
  fn new_wo(
    path: Arc<PathBuf>, page_size: usize, options: Self::WriteOptions,
  ) -> crate::Result<WOShell<Self>, IOError>;
}

pub trait NewIOReadWriter: NewIOReader + NewIOWriter {
  fn new_rw(
    path: Arc<PathBuf>, page_size: usize, read_options: Self::ReadOptions,
    write_options: Self::WriteOptions,
  ) -> crate::Result<Self, IOError>;
}

pub struct ROShell<R> {
  read: R,
}

impl<R> ROShell<R> {
  pub fn new(read: R) -> Self {
    Self { read }
  }
}

impl<R> IOBackend for ROShell<R>
where
  R: IOBackend,
{
  delegate! {
      to self.read {
          fn io_type(&self) -> IOType;
          fn page_size(&self) -> usize;
          fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), IOError>;
      }
  }
}
impl<R> IOReader for ROShell<R>
where
  R: IOReader,
{
  type Bytes = R::Bytes;

  delegate! {
    to self.read {
      fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, IOError>;
      fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, IOError>;
    }
  }
}

impl<R> ROShell<R>
where
  R: NewIOReader,
{
  #[inline]
  pub fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: R::ReadOptions,
  ) -> crate::Result<Self, IOError> {
    R::new_ro(path, page_size, options)
  }
}

impl<R> ContigIOReader for ROShell<R>
where
  R: ContigIOReader,
{
  delegate! {
    to self.read {
      fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, IOError>;
      fn read_contig_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, IOError>;
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

impl<W> IOBackend for WOShell<W>
where
  W: IOBackend,
{
  delegate! {
      to self.write {
          fn io_type(&self) -> IOType;
          fn page_size(&self) -> usize;
          fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), IOError>;
      }
  }
}

impl<W> IOWriter for WOShell<W>
where
  W: IOWriter,
{
  delegate! {
    to self.write {
      fn write_single_page(
          &self, disk_page_id: DiskPageId, page: SharedBytes,
        ) -> crate::Result<(), IOError>;
    }
  }
}

impl<W> WOShell<W>
where
  W: NewIOWriter,
{
  #[inline]
  fn new_wo(
    path: Arc<PathBuf>, page_size: usize, options: W::WriteOptions,
  ) -> crate::Result<Self, IOError> {
    W::new_wo(path, page_size, options)
  }
}

pub struct RWShell<R, W> {
  read: ROShell<R>,
  write: WOShell<W>,
}

impl<R, W> RWShell<R, W>
where
  R: IOReader,
  W: IOWriter,
{
  pub fn new(read: ROShell<R>, write: WOShell<W>) -> Self {
    assert_eq!(read.page_size(), write.page_size());
    Self { read, write }
  }
}

impl<R, W> RWShell<R, W>
where
  R: NewIOReader,
  W: NewIOWriter,
{
  fn new_rw(
    path: Arc<PathBuf>, page_size: usize, read_options: R::ReadOptions,
    write_options: W::WriteOptions,
  ) -> crate::Result<Self, IOError> {
    let read = R::new_ro(path.clone(), page_size, read_options)?;
    let write = W::new_wo(path, page_size, write_options)?;
    Ok(RWShell::new(read, write))
  }
}

impl<R, W> IOBackend for RWShell<R, W>
where
  R: IOBackend,
  W: IOBackend,
{
  #[inline]
  fn io_type(&self) -> IOType {
    IOType::RW
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.read.page_size()
  }

  fn apply_length_update(&mut self, new_len: usize) -> error_stack::Result<(), IOError> {
    self.read.apply_length_update(new_len)?;
    self.write.apply_length_update(new_len)
  }
}

impl<R, W> IOReader for RWShell<R, W>
where
  R: IOReader,
  W: IOWriter,
{
  type Bytes = R::Bytes;

  delegate! {
    to self.read {
      fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, IOError>;
      fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, IOError>;
    }
  }
}

impl<R, W> ContigIOReader for RWShell<R, W>
where
  R: ContigIOReader,
  W: IOWriter,
{
  delegate! {
    to self.read {
      fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, IOError>;
      fn read_contig_page(&self, disk_page_id: DiskPageId) -> crate::Result<Self::Bytes, IOError>;
    }
  }
}

impl<R, W> IOWriter for RWShell<R, W>
where
  R: IOReader,
  W: IOWriter,
{
  delegate! {
    to self.write {
      fn write_single_page(
          &self, disk_page_id: DiskPageId, page: SharedBytes,
        ) -> crate::Result<(), IOError>;
    }
  }
}

pub struct DirectReadHandler<T, I> {
  pub(crate) tx_context: T,
  pub(crate) io: I,
}

pub trait IOPageReader {
  type Bytes: IOBytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, IOError>;

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, IOError>;

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, IOError>;
}

impl<T, I> IOPageReader for DirectReadHandler<T, I>
where
  T: TxDirectContext,
  I: ContigIOReader,
{
  type Bytes = I::Bytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, IOError> {
    let disk_page_id = self.tx_context.trans_meta_id(meta_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, IOError> {
    let disk_page_id = self.tx_context.trans_freelist_id(freelist_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, IOError> {
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
  ) -> crate::Result<Self::Bytes, IOError>;

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, IOError>;
}

pub struct CachedReadHandler<T, I: IOReader<Bytes = SharedBytes>> {
  pub(crate) handler: DirectReadHandler<T, I>,
  pub(crate) page_cache: Cache<DiskPageId, SharedBytes>,
}

const TRY_AND_COMPUTE: bool = false;

impl<T, I> CachedReadHandler<T, I>
where
  T: TxContext,
  I: IOReader<Bytes = SharedBytes>,
{
  fn read_cache_or_disk(&self, disk_page_id: DiskPageId) -> crate::Result<SharedBytes, IOError> {
    if TRY_AND_COMPUTE {
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
    } else {
      self
        .page_cache
        .try_get_with(disk_page_id, || {
          self.handler.io.read_single_page(disk_page_id)
        })
        .map_err(|source| {
          let report: Report<IOError> = IOError::ReadError(disk_page_id).into();
          report.attach_printable(source)
        })
    }
  }
}

impl<T, I> IOPageReader for CachedReadHandler<T, I>
where
  T: TxContext,
  I: IOReader<Bytes = SharedBytes>,
{
  type Bytes = SharedBytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, IOError> {
    let disk_page_id = self.handler.tx_context.trans_meta_id(meta_page_id);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, IOError> {
    let disk_page_id = self.handler.tx_context.trans_freelist_id(freelist_page_id);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, IOError> {
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
  ) -> crate::Result<Self::Bytes, IOError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_freelist_id(freelist_page_id + overflow);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, IOError> {
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
      -> crate::Result<Self::Bytes, IOError>;
        fn read_freelist_page(&self, freelist_page_id: FreelistPageId)
      -> crate::Result<Self::Bytes, IOError>;
        fn read_node_page(&self, node_page_id: NodePageId)
      -> crate::Result<Self::Bytes, IOError>;
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
      -> crate::Result<Self::Bytes, IOError>;
        fn read_node_overflow(&self, node_page_id: NodePageId, overflow: u32,)
      -> crate::Result<Self::Bytes, IOError>;
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
      -> crate::Result<Self::Bytes, IOError>;
        fn read_freelist_page(&self, freelist_page_id: FreelistPageId)
      -> crate::Result<Self::Bytes, IOError>;
        fn read_node_page(&self, node_page_id: NodePageId)
      -> crate::Result<Self::Bytes, IOError>;
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
      -> crate::Result<Self::Bytes, IOError>;
        fn read_node_overflow(&self, node_page_id: NodePageId, overflow: u32,)
      -> crate::Result<Self::Bytes, IOError>;
    }
  }
}
