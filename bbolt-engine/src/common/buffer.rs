use crate::common::buffer_pool::PoolCommand;
use crate::common::page::PageHeader;
use aligners::{AlignedBytes, alignment};
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct OwnedBuffer {
  pub(crate) inner: Option<Arc<OwnedBufferInner>>,
}

unsafe impl Send for OwnedBuffer {}
unsafe impl Sync for OwnedBuffer {}

impl Deref for OwnedBuffer {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    self.inner.as_deref().unwrap()
  }
}

pub struct OwnedBufferInner {
  page: AlignedBytes<alignment::Page>,
  tx: Option<Sender<PoolCommand>>,
}

unsafe impl Send for OwnedBufferInner {}
unsafe impl Sync for OwnedBufferInner {}

impl OwnedBufferInner {
  pub fn new(page: AlignedBytes<alignment::Page>) -> Arc<Self> {
    Arc::new(OwnedBufferInner { page, tx: None })
  }

  pub fn new_with_tx(page: AlignedBytes<alignment::Page>, tx: Sender<PoolCommand>) -> Arc<Self> {
    Arc::new(OwnedBufferInner { page, tx: Some(tx) })
  }

  pub(crate) fn reset(&mut self) {
    self.page.fill(0)
  }
}

impl Deref for OwnedBufferInner {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.page
  }
}

impl DerefMut for OwnedBufferInner {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.page
  }
}

impl Drop for OwnedBuffer {
  fn drop(&mut self) {
    if let Some(inner) = self.inner.take() {
      if (1, 0) == (Arc::strong_count(&inner), Arc::weak_count(&inner)) {
        if let Some(tx) = inner.tx.clone() {
          tx.send(PoolCommand::Push(inner)).unwrap()
        }
      }
    }
  }
}

#[derive(Clone)]
pub enum PageBuffer<'tx> {
  Owned(OwnedBuffer),
  Mapped(&'tx [u8]),
}

impl<'tx> PageBuffer<'tx> {
  pub fn owned(buffer: Arc<OwnedBufferInner>) -> PageBuffer<'tx> {
    PageBuffer::Owned(OwnedBuffer {
      inner: Some(buffer),
    })
  }

  pub fn owned_bytes(buffer: AlignedBytes<alignment::Page>) -> PageBuffer<'tx> {
    Self::owned(OwnedBufferInner::new(buffer))
  }

  pub fn get_header(&self) -> &PageHeader {
    bytemuck::from_bytes(&self.slice(0, size_of::<PageHeader>()))
  }

  #[inline]
  pub fn slice(&self, offset: usize, len: usize) -> &[u8] {
    &self.deref()[offset..offset + len]
  }
}

impl<'tx> Deref for PageBuffer<'tx> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    match self {
      PageBuffer::Owned(o) => o,
      PageBuffer::Mapped(m) => *m,
    }
  }
}
