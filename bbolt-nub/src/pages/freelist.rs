use crate::common::id::FreelistPageId;
use crate::common::page::PageHeader;
use crate::pages::bytes::{TxPage};
use crate::pages::{HasHeader, Page};
use delegate::delegate;
use std::iter::RepeatN;
use crate::io::pages::HasRootPage;

pub trait HasFreelist: HasHeader {
  type FreelistIter: Iterator<Item = FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter;
}

#[derive(Clone)]
pub struct FreelistPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for FreelistPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T> HasHeader for FreelistPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn page_header(&self) -> &PageHeader;
      }
  }
}

impl<'tx, T> HasFreelist for FreelistPage<T>
where
  T: TxPage<'tx>,
{
  type FreelistIter = RepeatN<FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter {
    todo!()
  }
}
