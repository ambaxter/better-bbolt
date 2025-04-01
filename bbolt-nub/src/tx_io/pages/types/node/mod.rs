use crate::common::id::NodePageId;
use crate::tx_io::pages::types::node::branch::BranchPage;
use crate::tx_io::pages::types::node::leaf::LeafPage;
use crate::tx_io::pages::{Page, TxPageType};

pub mod branch;
pub mod leaf;

pub trait HasNode<'tx> {
  type KvSlice: KvDataType + 'tx;

  fn search(&self, v: &[u8]) -> usize;
  fn key(&self, index: usize) -> Option<Self::KvSlice>;
}

pub trait HasBranch<'tx>: HasNode<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasLeaf<'tx>: HasNode<'tx> {
  fn value(&self, index: usize) -> Option<Self::KvSlice>;
}

pub enum NodePage<'tx, T: 'tx> {
  Branch(BranchPage<'tx, T>),
  Leaf(LeafPage<'tx, T>),
}

impl<'tx, T: 'tx> Page for NodePage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn root_page(&self) -> &[u8] {
    match self {
      Self::Branch(page) => page.root_page(),
      Self::Leaf(page) => page.root_page(),
    }
  }
}
