use crate::common::id::NodePageId;
use crate::common::page::PageHeader;
use crate::common::page_bytes::PageBytes;
use crate::pages::HasHeader;
use crate::pages::meta::Meta;

pub trait HasNode : HasHeader {
  type ByteType<'a>: PageBytes where Self: 'a;
  fn search(&self, v: &[u8]) -> Option<usize>;
  fn key<'a>(&'a self, index: usize) -> Option<Self::ByteType<'a>>;
}

pub trait HasBranch: HasNode {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasLeaf : HasNode {
  fn value<'a>(&'a self, index: usize) -> Option<Self::ByteType<'a>>;
}


#[derive(Clone)]
struct Floof;

impl HasHeader for Floof {
  fn page_header(&self) -> &PageHeader {
    unimplemented!()
  }
}

impl HasNode for Floof {
  type ByteType<'a> = & 'a [u8];

  fn search(&self, v: &[u8]) -> Option<usize> {
    todo!()
  }

  fn key<'a>(&'a self, index: usize) -> Option<Self::ByteType<'a>> {
    todo!()
  }
}

#[derive(Clone)]
pub enum NodeType<B,L> {
  Branch(B),
  Leaf(L),
}

impl<B, L> HasHeader for NodeType<B,L> where B: HasHeader, L: HasHeader {
  fn page_header(&self) -> &PageHeader {
    match self {
      NodeType::Branch(b) => b.page_header(),
      NodeType::Leaf(l) => l.page_header(),
    }
  }
}