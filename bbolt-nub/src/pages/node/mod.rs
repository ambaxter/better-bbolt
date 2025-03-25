use crate::api::bytes::TxSlice;
use crate::common::id::NodePageId;
use crate::common::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, SubSlice, TxPage};
use std::ops::{Deref, RangeBounds};

pub mod branch;
pub mod leaf;

pub trait HasNode<'tx>: TxPage<'tx> {
  fn search(&self, v: &[u8]) -> Option<usize>;
  fn key(&self, index: usize) -> Option<Self::OutputSlice>;
}

pub trait HasBranch<'tx>: HasNode<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasLeaf<'tx>: HasNode<'tx> {
  fn value(&self, index: usize) -> Option<Self::OutputSlice>;
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

impl<'tx, B, L> SubSlice<'tx> for NodePage<B, L>
where
  B: HasNode<'tx>,
  L: HasNode<'tx, OutputSlice=B::OutputSlice>,
{
  type OutputSlice = B::OutputSlice;

  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::OutputSlice {
    match self {
      NodePage::Branch(b) => b.sub_slice(range),
      NodePage::Leaf(l) => l.sub_slice(range),
    }
  }
}

impl<'tx, B, L> TxPage<'tx> for NodePage<B, L> where B: HasNode<'tx>, L: HasNode<'tx, OutputSlice=B::OutputSlice>, {}


impl<'tx, B, L> HasNode<'tx> for NodePage<B, L>
where
  B: HasNode<'tx>,
  L: HasNode<'tx, OutputSlice = B::OutputSlice>,
{

  fn search(&self, v: &[u8]) -> Option<usize> {
    match &self {
      NodePage::Branch(branch) => branch.search(v),
      NodePage::Leaf(leaf) => leaf.search(v),
    }
  }

  fn key(&self, index: usize) -> Option<Self::OutputSlice> {
    match &self {
      NodePage::Branch(branch) => branch.key(index),
      NodePage::Leaf(leaf) => leaf.key(index),
    }
  }
}
