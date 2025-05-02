use crate::common::buffer_pool::BufferPool;
use crate::common::errors::IOError;
use crate::common::id::DiskPageId;
use crate::common::layout::page::PageHeader;
use crate::io::backends::{IOBackend, IOReader, ROShell, WOShell};
use crate::io::bytes::IOBytes;
use crate::io::bytes::shared_bytes::SharedBytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
  type WriteError: Sync + Send + 'static;
  type WriteOptions: Clone + Sized;
  fn new_wo(
    path: Arc<PathBuf>, page_size: usize, options: Self::WriteOptions,
  ) -> crate::Result<WOShell<Self>, Self::WriteError>;

  fn write_single_page(
    &self, disk_page_id: DiskPageId, page: SharedBytes,
  ) -> crate::Result<(), Self::WriteError>;
}

pub trait IOReadWriter: IOReader + IOWriter {
  type RWError: Sync + Send + 'static;

  fn new_rw(
    path: Arc<PathBuf>, page_size: usize, read_options: Self::ReadOptions,
    write_options: Self::WriteOptions,
  ) -> crate::Result<Self, Self::RWError>;
}
