use crate::common::buffer_pool::BufferPool;
use crate::common::errors::DiskError;
use crate::common::id::DiskPageId;
use crate::io::backends::{IOBackend, IOReader, IOType, IOWriter, ROShell, WOShell};
use crate::io::bytes::shared_bytes::SharedBytes;
use error_stack::ResultExt;
use std::fs::{File, OpenOptions};
use std::mem::offset_of;
use std::os::unix::fs::FileExt;
use std::os::unix::raw::off_t;
use std::path::Path;

#[derive(Clone)]
pub struct PFileOptions {
  buffer_pool: Option<BufferPool>,
}

pub struct PFileIO {
  io_type: IOType,
  file: File,
  buffer_pool: Option<BufferPool>,
  page_size: usize,
}

impl IOBackend for PFileIO {
  type ConfigOptions = PFileOptions;

  #[inline]
  fn io_type(&self) -> IOType {
    self.io_type
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.page_size
  }

  #[inline]
  fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), DiskError> {
    Ok(())
  }
}

impl PFileIO {
  pub fn new_rw<P: AsRef<Path>>(
    path: P, buffer_pool: BufferPool, page_size: usize,
  ) -> crate::Result<Self, DiskError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .open(path)
      .change_context(DiskError::OpenError(path.into()))?;
    Ok(PFileIO {
      io_type: IOType::RW,
      file,
      buffer_pool: Some(buffer_pool),
      page_size,
    })
  }

  pub fn new_ro<P: AsRef<Path>>(
    path: P, buffer_pool: BufferPool, page_size: usize,
  ) -> crate::Result<ROShell<Self>, DiskError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
      .read(true)
      .open(path)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    let p_file = PFileIO {
      io_type: IOType::RO,
      file,
      buffer_pool: Some(buffer_pool),
      page_size,
    };
    Ok(ROShell::new(p_file))
  }

  pub fn new_wo<P: AsRef<Path>>(
    path: P, page_size: usize,
  ) -> crate::Result<WOShell<Self>, DiskError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
      .write(true)
      .open(path)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    let p_file = PFileIO {
      io_type: IOType::WO,
      file,
      buffer_pool: None,
      page_size,
    };
    Ok(WOShell::new(p_file))
  }
}

impl IOReader for PFileIO {
  type Bytes = SharedBytes;

  fn page_size(&self) -> usize {
    self.page_size
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let page_offset = disk_page_id.0 * self.page_size as u64;
    let mut buffer = self
      .buffer_pool
      .as_ref()
      .expect("must be set to read")
      .pop_with_len(page_len);
    buffer
      .read_exact_at_and_share(&self.file, page_offset)
      .change_context(DiskError::ReadError(disk_page_id))
  }
}

impl IOWriter for PFileIO {
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
