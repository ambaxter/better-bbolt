use crate::backend::{PagingBackend, ReadHandle, WriteHandle};
use crate::common::buffer::PageBuffer;
use crate::common::ids::PageId;
use crate::common::page::PageHeader;
use aligners::{alignment, AlignedBytes};
use bytemuck::bytes_of_mut;
use parking_lot::{RwLockReadGuard, RwLockUpgradableReadGuard};
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::slice_from_raw_parts;
use std::rc::Rc;

pub struct MemoryBackend {
  bytes: AlignedBytes<alignment::Page>,
  page_size: usize,
}

unsafe impl Send for MemoryBackend {}
unsafe impl Sync for MemoryBackend {}

impl MemoryBackend {
  pub fn new(page_size: usize, bytes: AlignedBytes<alignment::Page>) -> MemoryBackend {
    MemoryBackend { page_size, bytes }
  }
}

pub enum LockedBytes<'tx> {
  Read(RwLockReadGuard<'tx, MemoryBackend>),
  URead(RwLockUpgradableReadGuard<'tx, MemoryBackend>),
}

impl<'tx> LockedBytes<'tx> {
  fn page_in(&self, page_id: PageId) -> std::io::Result<PageBuffer<'tx>> {
    let backend = match self {
      LockedBytes::Read(b) => b.deref(),
      LockedBytes::URead(b) => b.deref(),
    };
    let mut header = PageHeader::default();
    let offset = page_id.0 as usize * backend.page_size;
    bytes_of_mut(&mut header)
      .copy_from_slice(&backend.bytes[offset..offset + size_of::<PageHeader>()]);
    let page_len = backend.page_size * (header.overflow() as usize + 1);
    let bytes_ptr = backend.bytes.as_ptr();
    // Safety: Page will be valid for the entirety of the transaction
    assert!(backend.bytes.len() >= offset + page_len);
    Ok(PageBuffer::Mapped(unsafe {
      &*slice_from_raw_parts(bytes_ptr.add(offset), page_len)
    }))
  }
}

impl PagingBackend for MemoryBackend {
  type RHandle<'a> = MemoryReadHandle<'a>;
  type RWHandle<'a> = MemoryWriteHandle<'a>;

  fn read_handle<'a>(lock: RwLockReadGuard<'a, Self>) -> Self::RHandle<'a> {
    MemoryReadHandle {
      backend: Rc::new(LockedBytes::Read(lock)),
    }
  }

  fn write_handle<'a>(lock: RwLockUpgradableReadGuard<'a, Self>) -> Self::RWHandle<'a> {
    todo!()
  }
}

#[derive(Clone)]
pub struct MemoryReadHandle<'tx> {
  backend: Rc<LockedBytes<'tx>>,
}

impl<'tx> ReadHandle<'tx> for MemoryReadHandle<'tx> {
  fn page_in(&self, page_id: PageId) -> std::io::Result<PageBuffer<'tx>> {
    self.backend.page_in(page_id)
  }
}

pub struct MemoryWriteHandle<'tx> {
  p: PhantomData<&'tx [u8]>,
}
impl<'a> ReadHandle<'a> for MemoryWriteHandle<'a> {
  fn page_in(&self, page_id: PageId) -> std::io::Result<PageBuffer<'a>> {
    todo!()
  }
}

impl<'a> WriteHandle<'a> for MemoryWriteHandle<'a> {
  fn write<T: Into<Vec<u8>>>(&mut self, pages: Vec<(PageId, T)>) {
    todo!()
  }
}
