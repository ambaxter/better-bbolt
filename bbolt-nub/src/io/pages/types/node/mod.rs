use crate::common::id::NodePageId;
use crate::io::pages::types::node::branch::BranchPage;
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, KvDataType, Page, TxPageType};

pub mod branch;
pub mod cursor;
pub mod leaf;

pub trait HasNode<'tx> {
  type RefKv<'a>: GetKvRefSlice + KvDataType + 'a
  where
    Self: 'a;
  type TxKv: GetKvTxSlice<'tx> + KvDataType + 'tx;

  fn search(&self, v: &[u8]) -> usize;
  fn key_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>>;
  fn key(&self, index: usize) -> Option<Self::TxKv>;
}

pub trait HasBranch<'tx>: HasNode<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasLeaf<'tx>: HasNode<'tx> {
  fn value_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>>;
  fn value(&self, index: usize) -> Option<Self::TxKv>;
}

pub enum NodePage<'tx, T: 'tx> {
  Branch(BranchPage<'tx, T>),
  Leaf(LeafPage<'tx, T>),
}

impl<'tx, T: 'tx> NodePage<'tx, T> {
  pub fn is_leaf(&self) -> bool {
    matches!(self, NodePage::Leaf(_))
  }

  pub fn is_branch(&self) -> bool {
    matches!(self, NodePage::Branch(_))
  }
}

impl<'tx, T: 'tx> NodePage<'tx, T>
where
  T: TxPageType<'tx>,
{
  pub fn len(&self) -> usize {
    let len = match self {
      NodePage::Branch(branch) => branch.page_header().count(),
      NodePage::Leaf(leaf) => leaf.page_header().count(),
    };
    len as usize
  }
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
