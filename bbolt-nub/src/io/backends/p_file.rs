use crate::common::buffer_pool::BufferPool;
use crate::common::errors::DiskError;
use crate::common::id::DiskPageId;
use crate::io::backends::{IOReader, IOWriter};
use crate::io::bytes::shared_bytes::SharedBytes;
use error_stack::ResultExt;
use std::fs::File;
use std::mem::offset_of;
use std::os::unix::fs::FileExt;
use std::os::unix::raw::off_t;

pub struct PFileReader {
  file: File,
  buffer_pool: BufferPool,
  page_size: usize,
}

impl IOReader for PFileReader {
  type Bytes = SharedBytes;

  fn page_size(&self) -> usize {
    self.page_size
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let page_offset = disk_page_id.0 * self.page_size as u64;
    let mut buffer = self.buffer_pool.pop_with_len(page_len);
    buffer
      .read_exact_at_and_share(&self.file, page_offset)
      .change_context(DiskError::ReadError(disk_page_id))
  }
}

pub struct PFileWriter {
  file: File,
  page_size: usize,
}

impl IOWriter for PFileWriter {
  fn page_size(&self) -> usize {
    self.page_size
  }

  fn write_single_page(
    &self, disk_page_id: DiskPageId, page: SharedBytes,
  ) -> crate::Result<(), DiskError> {
    let page_offset = disk_page_id.0 * self.page_size as u64;
    assert_eq!(self.page_size, page.len());
    self
      .file
      .write_all_at(&page, page_offset)
      .change_context(DiskError::WriteError(disk_page_id))
  }
}
