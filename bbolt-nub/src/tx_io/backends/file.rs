use crate::common::buffer_pool::BufferPool;
use crate::common::errors::DiskReadError;
use crate::common::id::DiskPageId;
use crate::tx_io::backends::IOReader;
use crate::tx_io::bytes::shared_bytes::SharedBytes;
use crossbeam_channel::{Receiver, Sender};
use error_stack::ResultExt;
use parking_lot::Mutex;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
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
  ) -> crate::Result<Self, DiskReadError> {
    let file = File::open(&path)
      .change_context_lazy(|| DiskReadError::OpenError(path.as_ref().to_path_buf()))?;
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
    &self, disk_page_id: DiskPageId, page_offset: usize, page_len: usize,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let mut lock = self.file.lock();
    lock
      .seek(SeekFrom::Start(page_offset as u64))
      .and_then(|_| {
        let mut buffer = self.buffer_pool.pop_with_len(page_len);
        buffer.read_exact_and_share(&mut *lock)
      })
      .change_context(DiskReadError::ReadError(disk_page_id))
  }
}

pub struct MultiFileReader {
  path: PathBuf,
  tx: Sender<BufReader<File>>,
  rx: Receiver<BufReader<File>>,
  buffer_pool: BufferPool,
  page_size: usize,
}

impl MultiFileReader {
  fn new<P: AsRef<Path>>(
    path: P, reader_count: usize, page_size: usize, buffer_pool: BufferPool,
  ) -> crate::Result<Self, DiskReadError> {
    // TODO: Managing channels and File failures?
    // TODO: File locking?
    let (tx, rx) = crossbeam_channel::bounded(reader_count * 2);
    let path = path.as_ref().to_path_buf();
    for _ in 0..reader_count {
      let file =
        File::open(&path).change_context_lazy(|| DiskReadError::OpenError(path.clone()))?;
      let reader = BufReader::new(file);
      tx.send(reader)
        .change_context_lazy(|| DiskReadError::OpenError(path.clone()))?;
    }
    Ok(MultiFileReader {
      path,
      tx,
      rx,
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
    &self, disk_page_id: DiskPageId, page_offset: usize, page_len: usize,
  ) -> error_stack::Result<Self::Bytes, DiskReadError> {
    let mut file = self
      .rx
      .recv()
      .change_context(DiskReadError::ReadError(disk_page_id))?;
    let result = file
      .seek(SeekFrom::Start(page_offset as u64))
      .and_then(|_| {
        let mut buffer = self.buffer_pool.pop_with_len(page_len);
        buffer.read_exact_and_share(&mut file)
      })
      .change_context(DiskReadError::ReadError(disk_page_id));

    result.and_then(|buffer| {
      self
        .tx
        .send(file)
        .map(|_| buffer)
        .change_context(DiskReadError::ReadError(disk_page_id))
    })
  }
}
