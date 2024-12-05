use crate::backend::{PagingBackend, ReadHandle};
use crate::common::buffer::{OwnedBufferInner, PageBuffer};
use crate::common::buffer_pool::BufferPool;
use crate::common::ids::{NodePageId, PageId};
use crate::common::io_pool::{ReadPool, WritePool};
use crate::common::page::PageHeader;
use bytemuck::bytes_of_mut;
use dashmap::DashMap;
use memmap2::Mmap;
use moka::sync::Cache;
use parking_lot::{RwLock, RwLockReadGuard, RwLockUpgradableReadGuard};
use rayon::ThreadPool;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

pub enum ReadResult {
  Page(Arc<OwnedBufferInner>),
  Mmap(u32),
}

pub enum ReadType<'tx> {
  Cached(PageBuffer<'tx>),
  IO(PageBuffer<'tx>),
}

#[derive(Clone)]
pub enum BufferCacheEntry {
  Page(Arc<OwnedBufferInner>),
  Mmap(u32),
}

impl BufferCacheEntry {
  //TODO: Rename
  pub fn upgrade<'tx>(
    &self, page_size: usize, page_id: PageId, read_mmap: &'tx Mmap,
  ) -> PageBuffer<'tx> {
    match self {
      BufferCacheEntry::Page(buffer) => PageBuffer::owned(buffer.clone()),
      BufferCacheEntry::Mmap(len) => {
        let page_start = page_id.0 as usize * page_size;
        PageBuffer::Mapped(&read_mmap[page_start..page_start + *len as usize])
      }
    }
  }
}
pub struct FileBackend {
  path: PathBuf,
  page_size: usize,
  file_lock: File,
  read_mmap: Mmap,
  buffer_pool: Arc<BufferPool>,
  read_pool: ReadPool,
  write_pool: WritePool,
  file_threadpool: ThreadPool,
  cache: Cache<PageId, BufferCacheEntry>,
}

pub struct FileReadHandle<'p> {
  handle: RwLockReadGuard<'p, FileBackend>,
}

impl<'p> ReadHandle<'p> for FileReadHandle<'p> {
  fn page_in(&self, page_id: PageId) -> std::io::Result<PageBuffer<'p>> {
    todo!()
  }
}

pub struct FileWriteHandle<'a> {
  handle: RwLockUpgradableReadGuard<'a, FileBackend>,
}

impl FileBackend {
  pub fn page_in(&self, page_id: PageId) -> std::io::Result<ReadType> {
    let cached = self
      .cache
      .get(&page_id)
      .map(|cache| cache.upgrade(self.page_size, page_id, &self.read_mmap));
    if let Some(page) = cached {
      return Ok(ReadType::Cached(page));
    }
    let read_result = self.read_pool.read(&page_id, |reader| {
      let mut header = PageHeader::default();
      reader.seek(SeekFrom::Start(page_id.0 * self.page_size as u64))?;
      reader.read_exact(bytes_of_mut(&mut header))?;
      if header.overflow() > 0 {
        Ok(ReadResult::Mmap(header.overflow()))
      } else {
        reader.seek(SeekFrom::Start(page_id.0 * self.page_size as u64))?;
        let mut buffer = self.buffer_pool.pop();
        let buffer_mut = Arc::get_mut(&mut buffer).unwrap();
        reader.read_exact(buffer_mut)?;
        Ok(ReadResult::Page(buffer))
      }
    })?;
    let buffer = match read_result {
      ReadResult::Page(inner) => {
        self
          .cache
          .entry(page_id)
          .or_insert(BufferCacheEntry::Page(inner.clone()));
        PageBuffer::owned(inner)
      }
      ReadResult::Mmap(overflow) => {
        let page_start = page_id.0 as usize * self.page_size;
        let page_end = page_start + (self.page_size * ((overflow + 1) as usize));
        PageBuffer::Mapped(&self.read_mmap[page_start..page_end])
      }
    };
    Ok(ReadType::IO(buffer))
  }
}

#[cfg(test)]
mod test {}
