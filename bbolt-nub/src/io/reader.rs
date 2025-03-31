use crate::common::buffer_pool::BufferPool;
use crate::common::errors::DiskReadError;
use crate::common::id::DiskPageId;
use crate::common::page::PageHeader;
use crate::io::ReadData;
use crate::tx_io::bytes::shared_bytes::SharedBytes;
use bytemuck::bytes_of_mut;
use error_stack::ResultExt;
use std::io::{BufReader, Read, Seek, SeekFrom};

pub struct BaseReader<R>
where
  R: Read + Seek,
{
  reader: BufReader<R>,
  buffer_pool: BufferPool,
  page_size: usize,
}

impl<R> BaseReader<R>
where
  R: Read + Seek,
{
  fn new(reader: BufReader<R>, buffer_pool: BufferPool, page_size: usize) -> BaseReader<R> {
    BaseReader {
      reader,
      buffer_pool,
      page_size,
    }
  }

  fn peak_header(&mut self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, DiskReadError> {
    let mut header = PageHeader::default();
    self
      .reader
      .seek(SeekFrom::Start(disk_page_id.0 * self.page_size as u64))
      .and_then(|_| self.reader.read_exact(bytes_of_mut(&mut header)))
      .and_then(|_| self.reader.seek_relative(-(size_of::<PageHeader>() as i64)))
      .map(|_| header)
      .change_context(DiskReadError::ReadError(disk_page_id))
  }

  fn read_contig(&mut self, disk_page_id: DiskPageId) -> crate::Result<SharedBytes, DiskReadError> {
    let header = self.peak_header(disk_page_id)?;
    let mut shared = if header.get_overflow() == 0 {
      self.buffer_pool.pop()
    } else {
      let len = self.page_size * header.get_overflow() as usize;
      self.buffer_pool.pop_with_len(len)
    };
    shared
      .read_exact_and_share(&mut self.reader)
      .change_context(DiskReadError::ReadError(disk_page_id))
  }

  fn read_page(&mut self, disk_page_id: DiskPageId) -> crate::Result<SharedBytes, DiskReadError> {
    let shared = self.buffer_pool.pop();
    self
      .reader
      .seek(SeekFrom::Start(disk_page_id.0 * self.page_size as u64))
      .and_then(|_| shared.read_exact_and_share(&mut self.reader))
      .change_context(DiskReadError::ReadError(disk_page_id))
  }
}

impl<'tx, R: Read + Seek> ReadData<'tx> for BaseReader<R>
where
  Self: 'tx,
{
  type PageData = SharedBytes;

  fn read_disk(
    &self, disk_page_id: DiskPageId, pages: usize,
  ) -> crate::Result<Self::PageData, DiskReadError> {
    todo!()
  }
}
