use crate::common::buffer_pool::BufferPool;
use crate::common::errors::IOError;
use crate::common::id::DiskPageId;
use crate::io::backends::channel_store::ChannelStore;
use crate::io::backends::{IOBackend, IOCore, IOReader, IOType, NewIOReader, ROShell};
use crate::io::bytes::shared_bytes::SharedBytes;
use crossbeam_channel::{Receiver, Sender};
use error_stack::ResultExt;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct FileReadOptions {
  buffer_pool: BufferPool,
}

impl FileReadOptions {
  pub fn new(buffer_pool: BufferPool) -> FileReadOptions {
    FileReadOptions { buffer_pool }
  }
}

#[derive(Debug, Clone)]
pub struct FileWriteOptions {}

pub struct SingleFileIO {
  core: IOCore,
  file: Mutex<BufReader<File>>,
  buffer_pool: Option<BufferPool>,
}

impl SingleFileIO {
  fn expect_read_resources(&self) -> &BufferPool {
    self.buffer_pool.as_ref().expect("must be set to read")
  }
}

impl IOBackend for SingleFileIO {
  #[inline]
  fn io_type(&self) -> IOType {
    self.core.io_type
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.core.page_size
  }
}

impl IOReader for SingleFileIO {
  type Bytes = SharedBytes;

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, IOError> {
    let buffer_pool = self.expect_read_resources();
    let page_offset = disk_page_id.0 * self.core.page_size as u64;
    let mut lock = self.file.lock();
    lock
      .seek(SeekFrom::Start(page_offset))
      .and_then(|_| {
        let mut buffer = buffer_pool.pop_with_len(page_len);
        buffer.read_exact_and_share(&mut *lock)
      })
      .change_context(IOError::ReadError(disk_page_id))
  }
}

impl NewIOReader for SingleFileIO {
  type ReadOptions = FileReadOptions;

  fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: Self::ReadOptions,
  ) -> error_stack::Result<ROShell<Self>, IOError> {
    let core = IOCore::new(path, page_size, IOType::RO);
    let file = core.open_file()?;
    Ok(ROShell::new(SingleFileIO {
      core,
      file: Mutex::new(BufReader::new(file)),
      buffer_pool: Some(options.buffer_pool),
    }))
  }
}

#[derive(Debug, Clone)]
pub struct MultiFileReadOptions {
  buffer_pool: BufferPool,
  reader_count: usize,
}

impl MultiFileReadOptions {
  pub fn new(buffer_pool: BufferPool, reader_count: usize) -> MultiFileReadOptions {
    MultiFileReadOptions { buffer_pool, reader_count }
  }
}

#[derive(Debug, Clone)]
pub struct MultiFileWriteOptions {
  writer_count: usize,
}

pub struct MultiFileIO {
  core: IOCore,
  read_channel: Option<ChannelStore<BufReader<File>>>,
  write_channel: Option<ChannelStore<File>>,
  buffer_pool: Option<BufferPool>,
}

impl IOBackend for MultiFileIO {
  #[inline]
  fn io_type(&self) -> IOType {
    self.core.io_type
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.core.page_size
  }
}

impl MultiFileIO {
  fn expect_read_resources(&self) -> (&ChannelStore<BufReader<File>>, &BufferPool) {
    self
      .read_channel
      .as_ref()
      .zip(self.buffer_pool.as_ref())
      .expect("must be set to read")
  }

  fn expect_write_resources(&self) -> &ChannelStore<File> {
    self.write_channel.as_ref().expect("must be set to write")
  }
}

impl IOReader for MultiFileIO {
  type Bytes = SharedBytes;

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, IOError> {
    let page_offset = disk_page_id.0 * self.core.page_size as u64;
    let (read_channel, buffer_pool) = self.expect_read_resources();
    let mut file = read_channel
      .pop()
      .change_context(IOError::ReadError(disk_page_id))?;
    file
      .seek(SeekFrom::Start(page_offset))
      .and_then(|_| {
        let mut buffer = buffer_pool.pop_with_len(page_len);
        buffer.read_exact_and_share(file.deref_mut())
      })
      .change_context(IOError::ReadError(disk_page_id))
  }
}

impl NewIOReader for MultiFileIO {
  type ReadOptions = MultiFileReadOptions;

  fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: Self::ReadOptions,
  ) -> crate::Result<ROShell<Self>, IOError> {
    let core = IOCore::new(path, page_size, IOType::RO);
    let read_channel = ChannelStore::new_with_capacity(options.reader_count);
    for _ in 0..options.reader_count {
      let file = core.open_file()?;
      let reader = BufReader::new(file);
      read_channel
        .push(reader)
        .change_context_lazy(|| IOError::OpenError(core.path().into()))?;
    }
    Ok(ROShell::new(MultiFileIO {
      core,
      read_channel: Some(read_channel),
      write_channel: None,
      buffer_pool: Some(options.buffer_pool),
    }))
  }
}
