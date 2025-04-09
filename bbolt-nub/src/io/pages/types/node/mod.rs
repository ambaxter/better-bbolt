use crate::common::errors::PageError;
use crate::common::id::NodePageId;
use crate::common::layout::node::{BranchElement, LeafElement};
use crate::common::layout::page::PageHeader;
use crate::io::ops::{GetKvRefSlice, GetKvTxSlice, KvDataType};
use crate::io::pages::types::node::branch::BranchPage;
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::{Page, TxPage, TxPageType};
use bytemuck::{Pod, cast_slice};
use std::ops::Range;
use std::ptr;

pub mod branch;
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

pub trait HasElements<'tx>: Page + GetKvRefSlice + Sync + Send {
  type Element: HasKeyPosLen + Sync;

  fn elements(&self) -> &[Self::Element] {
    let elements_len = self.page_header().count() as usize;
    let elements_start = size_of::<PageHeader>();
    let elements_end = elements_start + (elements_len * size_of::<Self::Element>());
    cast_slice(&self.root_page()[elements_start..elements_end])
  }

  fn key_range(&self, index: usize) -> Option<Range<usize>> {
    self.elements().get(index).map(|element| {
      let start = element.kv_data_start(index);
      let end = start + element.elem_key_len();
      start..end
    })
  }

  #[cfg(not(feature = "mt_search"))]
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

  // TODO: match closest vs match exact in 2 different traits
  // that way we can parallel as needed
  #[cfg(feature = "mt_search")]
  fn search(&self, v: &[u8]) -> Result<usize, usize> {
    use rayon::iter::IndexedParallelIterator;
    use rayon::iter::ParallelIterator;
    use rayon::slice::ParallelSlice;
    let elements = self.elements();
    let elements_start = elements.as_ptr().addr();
    let chunk_size = (elements.len() / rayon::current_num_threads()).min(16);
    let p = elements
      .par_chunks(chunk_size)
      .enumerate()
      .filter_map(|(chunk_index, chunk)| {
        let first = &chunk[0];
        let first_key_start = first.kv_data_start(0);
        let first_key = self.get_ref_slice(first_key_start..first_key_start + first.elem_key_len());
        if KvDataType::gt(&first_key, v) {
          None
        } else {
          Some((
            chunk_index,
            chunk.binary_search_by(|element| {
              let element_index =
                (ptr::from_ref(element).addr() - elements_start) / size_of::<Self::Element>();
              let key_start = element.kv_data_start(element_index);
              let key = self.get_ref_slice(key_start..key_start + element.elem_key_len());
              KvDataType::cmp(&key, v)
            }),
          ))
        }
      });
    let (chunk, result) = p
      .max_by_key(|chunk| chunk.0)
      .expect("iterator can't be empty");
    result
      .map(|index| (chunk * chunk_size) + index)
      .map_err(|index| (chunk * chunk_size) + index)
  }
}

pub trait HasNodes<'tx>: HasKeys<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasValues<'tx>: HasKeys<'tx> {
  fn value_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>>;
  fn value(&self, index: usize) -> Option<Self::TxKv>;
}

#[derive(Clone)]
pub enum NodePage<'tx, T> {
  Branch(BranchPage<'tx, T>),
  Leaf(LeafPage<'tx, T>),
}

impl<'tx, T> TryFrom<TxPage<'tx, T>> for NodePage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Error = PageError;

  fn try_from(value: TxPage<'tx, T>) -> Result<Self, Self::Error> {
    if value.page.page_header().is_leaf() {
      Ok(NodePage::Leaf(LeafPage::new(value)))
    } else if value.page.page_header().is_branch() {
      Ok(NodePage::Branch(BranchPage::new(value)))
    } else {
      Err(PageError::InvalidNodeFlag(value.page.page_header().flags()))
    }
  }
}

impl<'tx, T> NodePage<'tx, T> {
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
