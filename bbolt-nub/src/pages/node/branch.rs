use crate::common::page::PageHeader;
use crate::io::pages::{HasRootPage, TxPage};
use crate::pages::Page;
use delegate::delegate;

#[derive(Clone)]
pub struct BranchPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for BranchPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}
