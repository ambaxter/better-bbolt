use crate::api::bytes::TxSlice;
use crate::common::id::NodePageId;
use crate::common::layout::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, KvDataType, SubTxSlice, TxPage};
use crate::pages::node::branch::BranchPage;
use crate::pages::node::leaf::LeafPage;
use std::ops::{Deref, RangeBounds};

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

#[derive(Clone)]
pub enum NodePage<T> {
  Branch(BranchPage<T>),
  Leaf(LeafPage<T>),
}
