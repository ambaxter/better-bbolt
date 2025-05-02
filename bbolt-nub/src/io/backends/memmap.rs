use crate::common::errors::DiskError;
use crate::common::id::{DiskPageId, EOFPageId, FreelistPageId, MetaPageId, NodePageId};
use crate::common::layout::page::PageHeader;
use crate::io::backends::{
  ContigIOReader, IOBackend, IOOverflowPageReader, IOPageReader, IOReader, IOType, ROShell,
  ReadLoadedPageIO, WOShell,
};
use crate::io::bytes::ref_bytes::RefBytes;
use crate::io::pages::{TxReadLazyPageIO, TxReadPageIO};
use crate::io::transmogrify::{TxContext, TxDirectContext, TxIndirectContext};
use error_stack::ResultExt;
use memmap2::{Advice, Mmap, MmapOptions, MmapRaw};
use std::fs::{File, OpenOptions};
use std::mem;
use std::path::{Path, PathBuf};

pub struct MemMapOptions {
  pre_populate_pages: bool,
  use_mlock: bool,
}

pub struct MemMapIO {
  io_type: IOType,
  path: PathBuf,
  file: File,
  mmap: MmapRaw,
  options: MmapOptions,
  page_size: usize,
}

impl IOBackend for MemMapIO {
  type ConfigOptions = MemMapOptions;

  #[inline]
  fn io_type(&self) -> IOType {
    self.io_type
  }

  #[inline]
  fn page_size(&self) -> usize {
    self.page_size
  }

  fn apply_length_update(&mut self, new_len: usize) -> crate::Result<(), DiskError> {
    let options = self.options.clone();
    let mut new_mmap = match self.io_type {
      IOType::RO => options.map_raw_read_only(&self.file),
      _ => options.map_raw(&self.file),
    }
    .change_context_lazy(|| DiskError::OpenError(self.path.clone()))?;

    mem::swap(&mut self.mmap, &mut new_mmap);
    Ok(())
  }
}

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

impl ContigIOReader for MemMapReader {
  fn read_header(&self, disk_page_id: DiskPageId) -> crate::Result<PageHeader, DiskError> {
    let page_offset = disk_page_id.0 as usize * self.page_size;
    let header_end = page_offset + size_of::<PageHeader>();
    if header_end > self.mmap.len() {
      Err(
        DiskError::UnexpectedEOF(
          disk_page_id,
          EOFPageId(DiskPageId((self.mmap.len() / self.page_size) as u64)),
        )
        .into(),
      )
    } else {
      Ok(*bytemuck::from_bytes(&self.mmap[page_offset..header_end]))
    }
  }
}
