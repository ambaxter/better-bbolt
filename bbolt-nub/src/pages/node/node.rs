use crate::common::page::PageHeader;
use crate::pages::HasHeader;
use crate::pages::bytes::HasRootPage;
use crate::pages::node::HasNode;

#[derive(Clone)]
struct Floof;

impl HasRootPage for Floof {
  fn root_page(&self) -> &[u8] {
    todo!()
  }
}

impl HasHeader for Floof {
  fn page_header(&self) -> &PageHeader {
    unimplemented!()
  }
}

impl HasNode for Floof {
  type ByteType<'a> = &'a [u8];

  fn search(&self, v: &[u8]) -> Option<usize> {
    todo!()
  }

  fn key<'a>(&'a self, index: usize) -> Option<Self::ByteType<'a>> {
    todo!()
  }
}
