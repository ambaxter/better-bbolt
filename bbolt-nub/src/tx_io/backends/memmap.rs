use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, EOFPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::page::PageHeader;
use crate::tx_io::backends::{ReadEntireIO, ReadIO};
use crate::tx_io::bytes::ref_bytes::RefBytes;
use crate::tx_io::transmogrify::{TxContext, TxDirectContext, TxIndirectContext};
use memmap2::Mmap;

pub struct MemMapReader<T: TxContext> {
  trans_db_ids: T,
  mmap: Mmap,
  page_size: usize,
}

impl<T: TxContext> MemMapReader<T> {
  fn read_page(
    &self, disk_page_id: DiskPageId, page_offset: usize, page_len: usize,
  ) -> crate::Result<RefBytes, DiskReadError> {
    if page_offset + page_len > self.mmap.len() {
      let eof = EOFPageId(DiskPageId((self.mmap.len() / self.page_size) as u64));
      Err(DiskReadError::UnexpectedEOF(disk_page_id, eof).into())
    } else {
      let bytes = &self.mmap[page_offset..page_offset + page_len];
      Ok(RefBytes::from_ref(bytes))
    }
  }
}

impl<T> MemMapReader<T>
where
  T: TxDirectContext,
{
  fn read_entire_page(&self, disk_page_id: DiskPageId) -> crate::Result<RefBytes, DiskReadError> {
    let page_offset = disk_page_id.0 as usize * self.page_size;
    let header: &PageHeader =
      bytemuck::from_bytes(&self.mmap[page_offset..page_offset + size_of::<PageHeader>()]);
    let overflow = header.get_overflow();
    let page_len = self.page_size + (overflow + 1) as usize;
    self.read_page(disk_page_id, page_offset, page_len)
  }
}

impl<T> MemMapReader<T>
where
  T: TxIndirectContext,
{
  fn read_single_page(&self, disk_page_id: DiskPageId) -> crate::Result<RefBytes, DiskReadError> {
    let page_offset = disk_page_id.0 as usize * self.page_size;
    let page_len = self.page_size;
    self.read_page(disk_page_id, page_offset, page_len)
  }
}

pub struct MemMapEntireReader<T: TxDirectContext> {
  reader: MemMapReader<T>,
}

impl<T: TxDirectContext> ReadIO for MemMapEntireReader<T> {
  type Bytes = RefBytes;

  fn read_meta_page(&self, meta_page_id: MetaPageId) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.reader.trans_db_ids.trans_meta_id(meta_page_id);
    self.reader.read_entire_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.reader.trans_db_ids.trans_freelist_id(freelist_page_id);
    self.reader.read_entire_page(disk_page_id)
  }

  fn read_node_page(&self, node_page_id: NodePageId) -> crate::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.reader.trans_db_ids.trans_node_id(node_page_id);
    self.reader.read_entire_page(disk_page_id)
  }
}

pub struct MemMapLazyReader<T: TxIndirectContext> {
  reader: MemMapReader<T>,
}

impl<T: TxIndirectContext> ReadIO for MemMapLazyReader<T> {
  type Bytes = RefBytes;

  fn read_meta_page(
    &self, meta_page_id: MetaPageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.reader.trans_db_ids.trans_meta_id(meta_page_id);
    self.reader.read_single_page(disk_page_id)
  }

  fn read_freelist_page(
    &self, freelist_page_id: FreelistPageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.reader.trans_db_ids.trans_freelist_id(freelist_page_id);
    self.reader.read_single_page(disk_page_id)
  }

  fn read_node_page(
    &self, node_page_id: NodePageId,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let disk_page_id = self.reader.trans_db_ids.trans_node_id(node_page_id);
    self.reader.read_single_page(disk_page_id)
  }
}
