use crate::common::errors::PageError;
use crate::common::id::DbPageType;
use crate::common::page::PageHeader;
use std::ops::Deref;

#[derive(Debug, Clone)]
pub struct Slice<'a> {
  slice: &'a [u8],
}

impl<'a> Deref for Slice<'a> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.slice
  }
}

pub trait PageBytes: Clone + Deref<Target = [u8]> {}

pub struct Page<T: PageBytes> {
  buffer: T,
}

impl<T: PageBytes> Page<T> {
  pub fn new<D: DbPageType>(page_id: D, buffer: T) -> error_stack::Result<Self, PageError> {
    let p = Page { buffer };
    p.page_header().fast_check(page_id).map(|_| p)
  }

  pub fn page_header(&self) -> &PageHeader {
    bytemuck::from_bytes(&self.buffer[0..size_of::<PageHeader>()])
  }
}

impl<T: PageBytes> Deref for Page<T> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    &self.buffer
  }
}
