use crate::common::id::FreelistPageId;
use crate::common::page::PageHeader;
use crate::pages::bytes::HasRootPage;
use crate::pages::{HasHeader, Page, PageBytes};
use delegate::delegate;
use std::iter::RepeatN;

pub trait HasFreelist: HasHeader {
  type FreelistIter: Iterator<Item = FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter;
}

#[derive(Clone)]
pub struct FreelistPage<T: PageBytes> {
  page: Page<T>,
}

impl<T: PageBytes> HasRootPage for FreelistPage<T> {
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<T: PageBytes> HasHeader for FreelistPage<T> {
  delegate! {
      to &self.page {
          fn page_header(&self) -> &PageHeader;
      }
  }
}

impl<T: PageBytes> HasFreelist for FreelistPage<T> {
  type FreelistIter = RepeatN<FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter {
    todo!()
  }
}
