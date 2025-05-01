use crate::common::buffer_pool::BufferPool;
use crate::common::errors::DiskError;
use crate::common::id::DiskPageId;
use crate::io::backends::IOReader;
use crate::io::backends::channel_store::ChannelStore;
use crate::io::bytes::shared_bytes::SharedBytes;
use crossbeam_channel::{Receiver, Sender};
use error_stack::ResultExt;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::DerefMut;
use std::path::{Path, PathBuf};

pub struct SingleFileReader {
  path: PathBuf,
  file: Mutex<BufReader<File>>,
  buffer_pool: BufferPool,
  page_size: usize,
}

impl SingleFileReader {
  pub fn new<P: AsRef<Path>>(
    path: P, page_size: usize, buffer_pool: BufferPool,
  ) -> crate::Result<Self, DiskError> {
    let file = File::open(&path)
      .change_context_lazy(|| DiskError::OpenError(path.as_ref().to_path_buf()))?;
    let reader = BufReader::new(file);
    Ok(SingleFileReader {
      path: path.as_ref().to_path_buf(),
      file: Mutex::new(reader),
      buffer_pool,
      page_size,
    })
  }
}

impl IOReader for SingleFileReader {
  type Bytes = SharedBytes;

  fn page_size(&self) -> usize {
    self.page_size
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let page_offset = disk_page_id.0 * self.page_size as u64;
    let mut lock = self.file.lock();
    lock
      .seek(SeekFrom::Start(page_offset))
      .and_then(|_| {
        let mut buffer = self.buffer_pool.pop_with_len(page_len);
        buffer.read_exact_and_share(&mut *lock)
      })
      .change_context(DiskError::ReadError(disk_page_id))
  }
}

pub struct MultiFileReader {
  path: PathBuf,
  channel_store: ChannelStore<BufReader<File>>,
  buffer_pool: BufferPool,
  page_size: usize,
}

impl MultiFileReader {
  pub fn new<P: AsRef<Path>>(
    path: P, reader_count: usize, page_size: usize, buffer_pool: BufferPool,
  ) -> crate::Result<Self, DiskError> {
    let channel_store = ChannelStore::<BufReader<File>>::new_with_capacity(reader_count);
    let path = path.as_ref().to_path_buf();
    for _ in 0..reader_count {
      let file = File::open(&path).change_context_lazy(|| DiskError::OpenError(path.clone()))?;
      let reader = BufReader::new(file);
      channel_store
        .push(reader)
        .change_context_lazy(|| DiskError::OpenError(path.clone()))?;
    }
    Ok(MultiFileReader {
      path,
      channel_store,
      buffer_pool,
      page_size,
    })
  }
}

impl IOReader for MultiFileReader {
  type Bytes = SharedBytes;

  fn page_size(&self) -> usize {
    self.page_size
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let page_offset = disk_page_id.0 * self.page_size as u64;
    let mut file = self
      .channel_store
      .pop()
      .change_context(DiskError::ReadError(disk_page_id))?;
    file
      .seek(SeekFrom::Start(page_offset))
      .and_then(|_| {
        let mut buffer = self.buffer_pool.pop_with_len(page_len);
        buffer.read_exact_and_share(file.deref_mut())
      })
      .change_context(DiskError::ReadError(disk_page_id))
  }
}
