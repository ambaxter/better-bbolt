use crate::common::id::NodePageId;
use crate::common::layout::node::{BranchElement, LeafElement};
use crate::common::layout::page::PageHeader;
use crate::io::pages::types::node::branch::BranchPage;
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, KvDataType, Page, TxPageType};
use bytemuck::{Pod, cast_slice};
use std::ptr;

pub mod branch;
pub mod cursor;
pub mod leaf;

pub trait HasKeys<'tx> {
  type RefKv<'a>: GetKvRefSlice + KvDataType + 'a
  where
    Self: 'a;
  type TxKv: GetKvTxSlice<'tx> + KvDataType + 'tx;

  fn key_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>>;
  fn key(&self, index: usize) -> Option<Self::TxKv>;
}

pub trait HasKeyPosLen: Pod {
  fn elem_key_dist(&self) -> usize;

  fn elem_key_len(&self) -> usize;

  // Safety - index must be within the bounds of the element array
  #[inline]
  fn kv_data_start(&self, index: usize) -> usize {
    size_of::<PageHeader>() + (size_of::<Self>() * index) + self.elem_key_dist()
  }
}

impl HasKeyPosLen for BranchElement {
  #[inline]
  fn elem_key_dist(&self) -> usize {
    self.key_dist() as usize
  }

  #[inline]
  fn elem_key_len(&self) -> usize {
    self.key_len() as usize
  }
}

impl HasKeyPosLen for LeafElement {
  #[inline]
  fn elem_key_dist(&self) -> usize {
    self.key_dist() as usize
  }

  #[inline]
  fn elem_key_len(&self) -> usize {
    self.key_len() as usize
  }
}

pub trait HasElements<'tx>: Page + GetKvRefSlice {
  type Element: HasKeyPosLen;

  fn elements(&self) -> &[Self::Element] {
    let elements_len = self.page_header().count() as usize;
    let elements_start = size_of::<PageHeader>();
    let elements_end = elements_start + (elements_len * size_of::<Self::Element>());
    cast_slice(&self.root_page()[elements_start..elements_end])
  }

  fn search(&self, v: &[u8]) -> Result<usize, usize> {
    let elements = self.elements();
    let elements_start = elements.as_ptr().addr();
    elements.binary_search_by(|element| {
      let element_index =
        (ptr::from_ref(element).addr() - elements_start) / size_of::<Self::Element>();
      let key_start = element.kv_data_start(element_index);
      let key = self.get_ref_slice(key_start..key_start + element.elem_key_len());
      KvDataType::cmp(&key, v)
    })
  }

  fn search_closest(&self, v: &[u8]) -> usize {
    self.search(v).unwrap_or_else(|closest| closest)
  }
}

pub trait HasNodes<'tx>: HasKeys<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasValues<'tx>: HasKeys<'tx> {
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
