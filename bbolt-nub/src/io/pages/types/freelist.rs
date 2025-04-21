use crate::common::errors::PageError;
use crate::common::id::FreelistPageId;
use crate::io::pages::types::meta::MetaPage;
use crate::io::pages::{Page, TxPage, TxPageType};
use delegate::delegate;

pub trait HasFreelist {
  type FreelistIter: Iterator<Item = FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter;
}

pub struct FreelistPage<'tx, T> {
  page: TxPage<'tx, T>,
}

impl<'tx, T> TryFrom<TxPage<'tx, T>> for FreelistPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Error = PageError;

  fn try_from(value: TxPage<'tx, T>) -> Result<Self, Self::Error> {
    if value.page.page_header().is_meta() {
      Ok(FreelistPage { page: value })
    } else {
      Err(PageError::InvalidFreelistFlag(
        value.page.page_header().flags(),
      ))
    }
  }
}

impl<'tx, T> Page for FreelistPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}
