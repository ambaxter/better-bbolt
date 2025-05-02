use crate::common::errors::IOError;
use crate::common::id::{DiskPageId, EOFPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::backends::{
  ContigIOReader, IOBackend, IOCore, IOOverflowPageReader, IOPageReader, IOReader, IOType,
  NewIOReader, ROShell, ReadLoadedPageIO, WOShell,
};
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::pages::{TxReadLazyPageIO, TxReadPageIO};
use crate::io::transmogrify::{TxContext, TxDirectContext, TxIndirectContext};
use error_stack::ResultExt;
use memmap2::{Advice, Mmap, MmapOptions, MmapRaw};
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MemMapError {
  #[error("MemMap Unlock Failure")]
  MUnlockFailure,
  #[error("MemMap Lock Failure")]
  MLockFailure,
  #[error("MemMap Advice Failure")]
  AdviceFailure,
}

#[derive(Debug, Clone)]
pub struct MemMapReadOptions {
  pre_populate_pages: bool,
  use_mlock: bool,
  advise_random: bool,
}

#[derive(Debug, Clone)]
pub struct MemMapWriteOptions {}

pub struct MemMapIO {
  core: IOCore,
  file: File,
  mmap: MmapRaw,
  read_options: Option<MemMapReadOptions>,
}

impl IOBackend for MemMapIO {
  #[inline]
  fn io_type(&self) -> IOType {
    self.core.io_type
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.core.page_size
  }

  fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), IOError> {
    let mut options = MmapOptions::new();
    if let Some(read_options) = self.read_options.as_ref() {
      if read_options.pre_populate_pages {
        options.populate();
      }
    }
    let mut new_mmap = match self.core.io_type {
      IOType::RO => options.map_raw_read_only(&self.file),
      _ => options.map_raw(&self.file),
    }
    .change_context_lazy(|| IOError::OpenError((*self.core.path).clone()))?;

    mem::swap(&mut self.mmap, &mut new_mmap);

    if let Some(read_options) = self.read_options.as_mut() {
      if read_options.use_mlock {
        new_mmap
          .unlock()
          .change_context(MemMapError::MUnlockFailure)
          .change_context(IOError::UpdateLengthError)?;
        self
          .mmap
          .lock()
          .change_context(MemMapError::MLockFailure)
          .change_context(IOError::UpdateLengthError)?;
      }
      if read_options.advise_random {
        self
          .mmap
          .advise(Advice::Random)
          .change_context(MemMapError::AdviceFailure)
          .change_context(IOError::UpdateLengthError)?
      }
    }
    Ok(())
  }
}

impl IOReader for MemMapIO {
  type Bytes = RefBytes;

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> error_stack::Result<Self::Bytes, IOError> {
    let page_offset = disk_page_id.0 as usize * self.core.page_size;
    if page_offset + page_len > self.mmap.len() {
      let eof = EOFPageId(DiskPageId((self.mmap.len() / self.core.page_size) as u64));
      Err(IOError::UnexpectedEOF(disk_page_id, eof).into())
    } else {
      let ptr = unsafe { self.mmap.as_ptr().add(page_offset) };
      Ok(RefBytes::from_ptr_len(ptr, page_len))
    }
  }
}

impl ContigIOReader for MemMapIO {
  fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, IOError> {
    let page_offset = disk_page_id.0 as usize * self.core.page_size;
    let header_end = page_offset + size_of::<PageHeader>();
    if header_end > self.mmap.len() {
      let eof = EOFPageId(DiskPageId((self.mmap.len() / self.core.page_size) as u64));
      Err(IOError::UnexpectedEOF(disk_page_id, eof.into()).into())
    } else {
      let ptr = unsafe { self.mmap.as_ptr().add(page_offset) };
      let bytes = RefBytes::from_ptr_len(ptr, size_of::<PageHeader>());
      Ok(*bytemuck::from_bytes(bytes.as_ref()))
    }
  }
}

impl NewIOReader for MemMapIO {
  type ReadOptions = MemMapReadOptions;

  fn new_ro(
    path: Arc<PathBuf>, page_size: usize, options: Self::ReadOptions,
  ) -> error_stack::Result<ROShell<Self>, IOError> {
    let core = IOCore {
      path,
      page_size,
      io_type: IOType::RO,
    };
    let file = core.open_file()?;
    let mut mmap_options = MmapOptions::new();
    if options.pre_populate_pages {
      mmap_options.populate();
    }
    let mmap = mmap_options
      .map_raw_read_only(&file)
      .change_context_lazy(|| IOError::OpenError((*core.path).clone()))?;
    if options.advise_random {
      mmap
        .advise(Advice::Random)
        .change_context_lazy(|| IOError::OpenError((*core.path).clone()))?;
    }
    Ok(ROShell::new(MemMapIO {
      core,
      file,
      mmap,
      read_options: Some(options),
    }))
  }
}

/*
impl MemMapIO {
  pub fn new_rw<P: AsRef<Path>>(path: P, page_size: usize) -> crate::Result<Self, DiskError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .open(path)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    let options = MmapOptions::new();
    let mmap = options
      .clone()
      .map_raw(&file)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    Ok(Self {
      io_type: IOType::RW,
      path: path.into(),
      file,
      mmap,
      options,
      page_size,
    })
  }

  pub fn new_ro<P: AsRef<Path>>(
    path: P, page_size: usize,
  ) -> crate::Result<ROShell<Self>, DiskError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
      .read(true)
      .open(path)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    let options = MmapOptions::new();
    let mmap = options
      .clone()
      .map_raw_read_only(&file)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    Ok(ROShell::new(Self {
      io_type: IOType::RO,
      path: path.into(),
      file,
      mmap,
      options,
      page_size,
    }))
  }

  pub fn new_wo<P: AsRef<Path>>(
    path: P, page_size: usize,
  ) -> crate::Result<WOShell<Self>, DiskError> {
    let path = path.as_ref();
    let file = OpenOptions::new()
      .write(true)
      .open(path)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    let options = MmapOptions::new();
    let mmap = options
      .clone()
      .map_raw(&file)
      .change_context_lazy(|| DiskError::OpenError(path.into()))?;
    Ok(WOShell::new(Self {
      io_type: IOType::WO,
      path: path.into(),
      file,
      mmap,
      options,
      page_size,
    }))
  }
}

pub struct MemMapReader {
  mmap: Mmap,
  page_size: usize,
}

impl MemMapReader {
  pub fn new<P: AsRef<Path>>(path: P, page_size: usize) -> Self {
    let file = File::open(path.as_ref()).unwrap();
    let mmap = unsafe { MmapOptions::new().map(&file) }.unwrap();
    mmap.advise(Advice::Random).unwrap();
    Self { mmap, page_size }
  }
}

impl IOReader for MemMapReader {
  type Bytes = RefBytes;

  #[inline]
  fn page_size(&self) -> usize {
    self.page_size
  }

  fn read_disk_page(
    &self, disk_page_id: DiskPageId, page_len: usize,
  ) -> crate::Result<Self::Bytes, DiskError> {
    let page_offset = disk_page_id.0 as usize * self.page_size;
    if page_offset + page_len > self.mmap.len() {
      let eof = EOFPageId(DiskPageId((self.mmap.len() / self.page_size) as u64));
      Err(DiskError::UnexpectedEOF(disk_page_id, eof).into())
    } else {
      let bytes = &self.mmap[page_offset..page_offset + page_len];
      Ok(RefBytes::from_ref(bytes))
    }
  }
}


*/
