use crate::common::errors::DiskReadError;
use crate::common::id::{DiskPageId, EOFPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::backends::{
  ContigIOReader, IOOverflowPageReader, IOPageReader, IOReader, ReadLoadedPageIO,
};
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::pages::{ReadLazyPageIO, ReadPageIO};
use crate::io::transmogrify::{TxContext, TxDirectContext, TxIndirectContext};
use memmap2::Mmap;

pub struct MemMapReader {
  mmap: Mmap,
  page_size: usize,
}

impl IOReader for MemMapReader {
  type Bytes = RefBytes;

  #[inline]
  fn page_size(&self) -> usize {
    self.page_size
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_offset: usize, page_len: usize,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    if page_offset + page_len > self.mmap.len() {
      let eof = EOFPageId(DiskPageId((self.mmap.len() / self.page_size) as u64));
      Err(DiskReadError::UnexpectedEOF(disk_page_id, eof).into())
    } else {
      let bytes = &self.mmap[page_offset..page_offset + page_len];
      Ok(RefBytes::from_ref(bytes))
    }
  }
}

impl ContigIOReader for MemMapReader {
  fn read_header(
    &self, disk_page_id: DiskPageId,
  ) -> error_stack::Result<PageHeader, DiskReadError> {
    let page_offset = disk_page_id.0 as usize * self.page_size;
    let header_end = page_offset + size_of::<PageHeader>();
    if header_end > self.mmap.len() {
      Err(
        DiskReadError::UnexpectedEOF(
          disk_page_id,
          EOFPageId(DiskPageId((self.mmap.len() / self.page_size) as u64)),
        )
        .into(),
      )
    } else {
      Ok(*bytemuck::from_bytes(&self.mmap[page_offset..header_end]))
    }
  }
}
