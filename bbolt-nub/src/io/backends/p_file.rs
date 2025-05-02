use crate::common::buffer_pool::BufferPool;
use crate::common::errors::IOError;
use crate::common::id::DiskPageId;
use crate::io::backends::{IOBackend, IOCore, IOReader, IOType, IOWriter, ROShell, WOShell};
use crate::io::bytes::shared_bytes::SharedBytes;
use error_stack::ResultExt;
use std::fs::{File, OpenOptions};
use std::mem::offset_of;
use std::os::unix::fs::FileExt;
use std::os::unix::raw::off_t;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct PFileReadOptions {
  buffer_pool: BufferPool,
}

#[derive(Debug, Clone)]
pub struct PFileWriteOptions {}

pub struct PFileIO {
  core: IOCore,
  file: File,
  buffer_pool: Option<BufferPool>,
}

impl IOBackend for PFileIO {
  #[inline]
  fn io_type(&self) -> IOType {
    self.core.io_type
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.core.page_size
  }
}

impl IOReader for PFileIO {
  type Bytes = SharedBytes;
  type ReadOptions = PFileReadOptions;

  fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: Self::ReadOptions,
  ) -> error_stack::Result<ROShell<Self>, IOError> {
    let core = IOCore {
      path,
      page_size,
      io_type: IOType::RO,
    };
    let file = core.open_file()?;
    let p_file = PFileIO {
      core,
      file,
      buffer_pool: Some(options.buffer_pool),
    };
    Ok(ROShell::new(p_file))
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> error_stack::Result<Self::Bytes, IOError> {
    let page_offset = disk_page_id.0 * self.core.page_size as u64;
    let mut buffer = self
      .buffer_pool
      .as_ref()
      .expect("must be set to read")
      .pop_with_len(page_len);
    buffer
      .read_exact_at_and_share(&self.file, page_offset)
      .change_context(IOError::ReadError(disk_page_id))
  }
}
/*
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
*/
/*
impl IOWriter for PFileIO {
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
*/
