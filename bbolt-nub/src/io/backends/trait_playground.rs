use crate::common::buffer_pool::BufferPool;
use crate::common::errors::DiskError;
use crate::common::id::DiskPageId;
use crate::common::layout::page::PageHeader;
use crate::io::backends::{IOBackend, ROShell, WOShell};
use crate::io::bytes::IOBytes;
use crate::io::bytes::shared_bytes::SharedBytes;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub trait IOReader: IOBackend {
  type Bytes: IOBytes;
  fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: Self::ConfigOptions,
  ) -> crate::Result<ROShell<Self>, DiskError>;

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

pub trait IOWriter: IOBackend {
  fn new_wo(
    path: Arc<PathBuf>, page_size: usize, options: Self::ConfigOptions,
  ) -> crate::Result<WOShell<Self>, DiskError>;

  fn write_single_page(
    &self, disk_page_id: DiskPageId, page: SharedBytes,
  ) -> crate::Result<(), DiskError>;
}

pub trait IOReadWriter: IOReader + IOWriter {
  fn new_rw(
    path: Arc<PathBuf>, page_size: usize, options: Self::ConfigOptions,
  ) -> crate::Result<Self, DiskError>;
}
