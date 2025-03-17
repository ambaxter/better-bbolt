use crate::common::page::PageHeader;
use crate::pages::bytes::HasRootPage;
use crate::pages::node::NodePage;
use crate::pages::{HasHeader, PageBytes};
use delegate::delegate;

#[derive(Clone)]
pub struct LeafPage<T: PageBytes> {
  page: NodePage<T>,
}

impl<T: PageBytes> HasRootPage for LeafPage<T> {
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<T: PageBytes> HasHeader for LeafPage<T> {
  delegate! {
      to &self.page {
          fn page_header(&self) -> &PageHeader;
      }
  }
}
