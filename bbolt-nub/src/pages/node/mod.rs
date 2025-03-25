use crate::api::bytes::TxSlice;
use crate::common::id::NodePageId;
use crate::common::page::PageHeader;
use crate::pages::HasHeader;
use crate::pages::bytes::{TxPage};
use std::ops::Deref;
use crate::io::pages::HasRootPage;

pub mod branch;
pub mod leaf;

pub trait HasNode<'tx>: HasHeader {
  type SliceType: TxSlice<'tx>;
  fn search(&self, v: &[u8]) -> Option<usize>;
  fn key(&self, index: usize) -> Option<Self::SliceType>;
}

pub trait HasBranch<'tx>: HasNode<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasLeaf<'tx>: HasNode<'tx> {
  fn value(&self, index: usize) -> Option<Self::SliceType>;
}

#[derive(Clone)]
pub enum NodePage<B, L> {
  Branch(B),
  Leaf(L),
}

impl<B, L> HasRootPage for NodePage<B, L>
where
  B: HasRootPage,
  L: HasRootPage,
{
  fn root_page(&self) -> &[u8] {
    match self {
      NodePage::Branch(b) => b.root_page(),
      NodePage::Leaf(l) => l.root_page(),
    }
  }
}

impl<B, L> HasHeader for NodePage<B, L>
where
  B: HasHeader,
  L: HasHeader,
{
  fn page_header(&self) -> &PageHeader {
    match self {
      NodePage::Branch(b) => b.page_header(),
      NodePage::Leaf(l) => l.page_header(),
    }
  }
}

impl<'tx, B, L> HasNode<'tx> for NodePage<B, L>
where
  B: HasNode<'tx>,
  L: HasNode<'tx, SliceType = B::SliceType>,
{
  type SliceType = B::SliceType;

  fn search(&self, v: &[u8]) -> Option<usize> {
    match &self {
      NodePage::Branch(branch) => branch.search(v),
      NodePage::Leaf(leaf) => leaf.search(v),
    }
  }

  fn key(&self, index: usize) -> Option<Self::SliceType> {
    match &self {
      NodePage::Branch(branch) => branch.key(index),
      NodePage::Leaf(leaf) => leaf.key(index),
    }
  }
}
