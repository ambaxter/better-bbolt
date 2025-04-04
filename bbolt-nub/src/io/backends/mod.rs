use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::bytes::IOBytes;
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::bytes::shared_bytes::SharedBytes;
use crate::io::transmogrify::{TxContext, TxDirectContext};
use error_stack::Report;
use moka::Entry;
use moka::ops::compute::Op;
use moka::sync::Cache;
use std::sync::Arc;

pub mod file;
pub mod memmap;
pub mod meta_reader;

pub trait IOReader {
  type Bytes: IOBytes;

  fn page_size(&self) -> usize;

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_offset: usize, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_single_page(
    &self, disk_page_id: DiskPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let page_size = self.page_size();
    let page_offset = disk_page_id.0 as usize * page_size;
    let page_len = page_size;
    self.read_disk_page(disk_page_id, page_offset, page_len)
  }
}

pub trait ContigIOReader: IOReader {
  fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, DiskReadError>;
  fn read_contig_page(
    &self, disk_page_id: DiskPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let page_size = self.page_size();
    let page_offset = disk_page_id.0 as usize * page_size;
    let header = self.read_header(disk_page_id)?;
    let overflow = header.get_overflow();
    let page_len = page_size + (overflow + 1) as usize;
    self.read_disk_page(disk_page_id, page_offset, page_len)
  }
}

pub struct ReadHandler<T, I> {
  tx_context: T,
  io: I,
  page_size: usize,
}

pub trait IOPageReader {
  type Bytes: IOBytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError>;
}

impl<T, I> IOPageReader for ReadHandler<T, I>
where
  T: TxDirectContext,
  I: ContigIOReader,
{
  type Bytes = I::Bytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.tx_context.trans_meta_id(meta_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.tx_context.trans_freelist_id(freelist_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.tx_context.trans_node_id(node_page_id);
    self.io.read_contig_page(disk_page_id)
  }
}

pub trait ReadLoadedPageIO: IOPageReader {}

impl<T, I> ReadLoadedPageIO for ReadHandler<T, I>
where
  T: TxDirectContext,
  I: ContigIOReader,
{
}

pub trait IOOverflowPageReader: IOPageReader {
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;
}

pub struct CachedReadHandler<T, I: IOReader<Bytes = SharedBytes>> {
  handler: ReadHandler<T, I>,
  page_cache: Cache<DiskPageId, SharedBytes>,
}

impl<T, I> CachedReadHandler<T, I>
where
  T: TxContext,
  I: IOReader<Bytes = SharedBytes>,
{
  fn read_cache_or_disk(
    &self, disk_page_id: DiskPageId,
  ) -> crate::Result<SharedBytes, DiskReadError> {
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

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.handler.tx_context.trans_meta_id(meta_page_id);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.handler.tx_context.trans_freelist_id(freelist_page_id);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError> {
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
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_freelist_id(freelist_page_id + overflow);
    self.read_cache_or_disk(disk_page_id)
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_node_id(node_page_id + overflow);
    self.read_cache_or_disk(disk_page_id)
  }
}
