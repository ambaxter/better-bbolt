use crate::common::errors::PageError;
use crate::common::id::DbPageType;
use crate::common::page::PageHeader;
use crate::io::ReadData;
use crate::pages::bytes::{HasRootPage, TxPage};
use std::ops::{Deref, RangeBounds};

pub mod txpage;

pub mod bytes;

pub mod freelist;
pub mod meta;
pub mod node;

pub trait HasHeader: HasRootPage + Clone {
  fn page_header(&self) -> &PageHeader;
}

#[derive(Clone)]
pub struct Page<T> {
  buffer: T,
}

impl<'tx, T> Page<T>
where
  T: TxPage<'tx>,
{
  pub fn new<D: DbPageType>(page_id: D, buffer: T) -> error_stack::Result<Self, PageError> {
    let p = Page { buffer };
    p.page_header().fast_check(page_id).map(|_| p)
  }
}

impl<'tx, T> HasRootPage for Page<T>
where
  T: TxPage<'tx>,
{
  #[inline]
  fn root_page(&self) -> &[u8] {
    self.buffer.as_ref()
  }
}

impl<'tx, T> HasHeader for Page<T>
where
  T: TxPage<'tx>,
{
  fn page_header(&self) -> &PageHeader {
    bytemuck::from_bytes(&self.root_page()[0..size_of::<PageHeader>()])
  }
}
