use crate::common::id::FreelistPageId;
use crate::common::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, TxPage};
use crate::pages::Page;
use delegate::delegate;
use std::iter::RepeatN;

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

impl<'tx, T> HasFreelist for FreelistPage<T>
where
  T: TxPage<'tx>,
{
  type FreelistIter = RepeatN<FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter {
    todo!()
  }
}
