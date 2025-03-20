use crate::common::page::PageHeader;
use crate::pages::HasHeader;
use crate::pages::bytes::{HasRootPage, TxPage};
use crate::pages::node::NodePage;
use delegate::delegate;

#[derive(Clone)]
pub struct BranchPage<T> {
  page: NodePage<T>,
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

impl<'tx, T> HasHeader for BranchPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn page_header(&self) -> &PageHeader;
      }
  }
}
