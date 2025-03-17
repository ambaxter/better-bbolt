use crate::common::id::NodePageId;
use crate::common::page::PageHeader;
use crate::pages::bytes::HasRootPage;
use crate::pages::{HasHeader, Page, PageBytes};
use delegate::delegate;
use std::ops::Deref;

pub mod node;

pub mod branch;
pub mod leaf;

pub trait HasNode: HasHeader {
  type ByteType<'a>: PageBytes
  where
    Self: 'a;
  fn search(&self, v: &[u8]) -> Option<usize>;
  fn key<'a>(&'a self, index: usize) -> Option<Self::ByteType<'a>>;
}

pub trait HasBranch: HasNode {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasLeaf: HasNode {
  fn value<'a>(&'a self, index: usize) -> Option<Self::ByteType<'a>>;
}

#[derive(Clone)]
struct NodePage<T: PageBytes> {
  page: Page<T>,
}

impl<T: PageBytes> HasRootPage for NodePage<T> {
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<T: PageBytes> HasHeader for NodePage<T> {
  delegate! {
      to &self.page {
          fn page_header(&self) -> &PageHeader;
      }
  }
}

#[derive(Clone)]
pub enum NodeType<B, L>
where
  B: HasBranch,
  L: HasLeaf,
{
  Branch(B),
  Leaf(L),
}

impl<B, L> HasRootPage for NodeType<B, L>
where
  B: HasBranch,
  L: HasLeaf,
{
  fn root_page(&self) -> &[u8] {
    match self {
      NodeType::Branch(b) => b.root_page(),
      NodeType::Leaf(l) => l.root_page(),
    }
  }
}

impl<B, L> HasHeader for NodeType<B, L>
where
  B: HasBranch,
  L: HasLeaf,
{
  fn page_header(&self) -> &PageHeader {
    match self {
      NodeType::Branch(b) => b.page_header(),
      NodeType::Leaf(l) => l.page_header(),
    }
  }
}
