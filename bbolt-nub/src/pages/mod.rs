use crate::common::errors::PageError;
use crate::common::id::DbPageType;
use crate::common::page::PageHeader;
use crate::io::ReadPageData;
pub(crate) use crate::pages::bytes::PageBytes;
use std::ops::{Deref, RangeBounds};

pub mod bytes;

pub mod freelist;
pub mod meta;
pub mod node;

pub trait HasHeader: Clone {
  fn page_header(&self) -> &PageHeader;
}

#[derive(Clone)]
pub struct Page<T> {
  buffer: T,
}

impl<T: PageBytes> Page<T> {
  pub fn new<D: DbPageType>(page_id: D, buffer: T) -> error_stack::Result<Self, PageError> {
    let p = Page { buffer };
    p.page_header().fast_check(page_id).map(|_| p)
  }
}

impl<T: PageBytes> HasHeader for Page<T> {
  fn page_header(&self) -> &PageHeader {
    bytemuck::from_bytes(&self.buffer.as_ref()[0..size_of::<PageHeader>()])
  }
}

impl<T: PageBytes> Deref for Page<T> {
  type Target = [u8];

  fn deref(&self) -> &Self::Target {
    self.buffer.as_ref()
  }
}
