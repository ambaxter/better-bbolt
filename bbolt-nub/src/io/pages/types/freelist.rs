use crate::common::id::FreelistPageId;
use crate::io::pages::types::meta::MetaPage;
use crate::io::pages::{Page, TxPage, TxPageType};
use delegate::delegate;

pub trait HasFreelist {
  type FreelistIter: Iterator<Item = FreelistPageId>;

  fn freelist_iter(&self) -> Self::FreelistIter;
}

pub struct FreelistPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
}

impl<'tx, T: 'tx> Page for FreelistPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}
