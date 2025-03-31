use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::page::PageHeader;
use crate::tx_io::bytes::IOBytes;
use crate::tx_io::bytes::ref_bytes::RefBytes;
use crate::tx_io::transmogrify::{TxContext, TxDirectContext};

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

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.tx_context.trans_meta_id(meta_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.tx_context.trans_freelist_id(freelist_page_id);
    self.io.read_contig_page(disk_page_id)
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.tx_context.trans_node_id(node_page_id);
    self.io.read_contig_page(disk_page_id)
  }
}

pub trait ReadEntireIO: IOPageReader {}

impl<T, I> ReadEntireIO for ReadHandler<T, I>
where
  T: TxDirectContext,
  I: ContigIOReader,
{
}

pub trait IOSinglePageReader: IOPageReader {
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> crate::Result<Self::Bytes, DiskReadError>;
}

pub struct SinglePageReadHandler<T, I> {
  handler: ReadHandler<T, I>,
}

impl<T, I> IOPageReader for SinglePageReadHandler<T, I>
where
  T: TxContext,
  I: IOReader,
{
  type Bytes = I::Bytes;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.handler.tx_context.trans_meta_id(meta_page_id);
    self.handler.io.read_single_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.handler.tx_context.trans_freelist_id(freelist_page_id);
    self.handler.io.read_single_page(disk_page_id)
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.handler.tx_context.trans_node_id(node_page_id);
    self.handler.io.read_single_page(disk_page_id)
  }
}

impl<T, I> IOSinglePageReader for SinglePageReadHandler<T, I>
where
  T: TxContext,
  I: IOReader,
{
  fn read_freelist_overflow(
    &self, freelist_page_id: FreelistPageId, overflow: u32,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_freelist_id(freelist_page_id + overflow);
    self.handler.io.read_single_page(disk_page_id)
  }

  fn read_node_overflow(
    &self, node_page_id: NodePageId, overflow: u32,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self
      .handler
      .tx_context
      .trans_node_id(node_page_id + overflow);
    self.handler.io.read_single_page(disk_page_id)
  }
}
