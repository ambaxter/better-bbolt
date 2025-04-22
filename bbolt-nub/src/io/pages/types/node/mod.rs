use crate::common::errors::PageError;
use crate::common::layout::page::PageHeader;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::branch::bbolt::BBoltBranch;
use crate::io::pages::types::node::leaf::bbolt::BBoltLeaf;
use crate::io::pages::{GatKvRef, GetGatKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use branch::HasSearchBranch;
use bytemuck::{Pod, cast_slice};
use error_stack::ResultExt;
use ext::TrySliceExt;
use leaf::{HasSearchLeaf, HasValues};
use std::ops::Range;
use std::ptr;

pub mod ext;

pub mod branch;
pub mod leaf;

pub trait HasKeyRefs: GetGatKvRefSlice {
  fn key_ref<'a>(&'a self, index: usize) -> Option<<Self as GatKvRef<'a>>::KvRef>;
}

pub trait HasKeys<'tx>: HasKeyRefs {
  type TxKv: GetKvTxSlice<'tx>;

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

pub trait HasElements<'tx>: Page + HasKeyRefs + Sync + Send {
  type Element: HasKeyPosLen + Sync;

  #[inline]
  fn element_count(&self) -> usize {
    self.page_header().count() as usize
  }

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
  fn search<'a>(&'a self, v: &[u8]) -> Result<usize, usize>
  where
    <Self as GatKvRef<'a>>::KvRef: PartialOrd<[u8]>,
  {
    let elements = self.elements();
    assert!(!elements.is_empty());
    let elements_start = elements.as_ptr().addr();
    elements.binary_search_by(|element| {
      let element_index =
        (ptr::from_ref(element).addr() - elements_start) / size_of::<Self::Element>();
      let key_start = element.kv_data_start(element_index);
      let key = self.get_ref_slice(key_start..key_start + element.elem_key_len());
      PartialOrd::partial_cmp(&key, v).unwrap()
    })
  }

  #[cfg(not(feature = "mt_search"))]
  fn try_search<'a>(
    &'a self, v: &[u8],
  ) -> crate::Result<
    Result<usize, usize>,
    <<Self as GatKvRef<'a>>::KvRef as TryPartialEq<[u8]>>::Error,
  >
  where
    <Self as GatKvRef<'a>>::KvRef: TryPartialOrd<[u8]>,
  {
    let elements = self.elements();
    assert!(!elements.is_empty());
    let elements_start = elements.as_ptr().addr();
    elements.try_binary_search_by(|element| {
      let element_index =
        (ptr::from_ref(element).addr() - elements_start) / size_of::<Self::Element>();
      let key_start = element.kv_data_start(element_index);
      let key = self.get_ref_slice(key_start..key_start + element.elem_key_len());
      TryPartialOrd::try_partial_cmp(&key, v).map(|r| r.expect("never None"))
    })
  }

  // TODO: match closest vs match exact in 2 different traits
  // that way we can parallel as needed
  #[cfg(feature = "mt_search")]
  fn search<'a>(&'a self, v: &[u8]) -> Result<usize, usize>
  where
    <Self as GatKvRef<'a>>::KvRef: PartialOrd<[u8]>,
  {
    use rayon::iter::IndexedParallelIterator;
    use rayon::iter::ParallelIterator;
    use rayon::slice::ParallelSlice;
    let elements = self.elements();
    assert!(!elements.is_empty());
    let elements_start = elements.as_ptr().addr();
    let chunk_size = (elements.len() / rayon::current_num_threads()).min(16);
    let p = elements
      .par_chunks(chunk_size)
      .enumerate()
      .filter_map(|(chunk_index, chunk)| {
        let first = &chunk[0];
        let first_key_start = first.kv_data_start(0);
        let first_key = self.get_ref_slice(first_key_start..first_key_start + first.elem_key_len());
        if PartialOrd::gt(&first_key, v) {
          None
        } else {
          Some((
            chunk_index,
            chunk.binary_search_by(|element| {
              let element_index =
                (ptr::from_ref(element).addr() - elements_start) / size_of::<Self::Element>();
              let key_start = element.kv_data_start(element_index);
              let key = self.get_ref_slice(key_start..key_start + element.elem_key_len());
              PartialOrd::partial_cmp(&key, v).unwrap()
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

  #[cfg(feature = "mt_search")]
  fn try_search<'a>(
    &'a self, v: &[u8],
  ) -> crate::Result<
    Result<usize, usize>,
    <<Self as GatKvRef<'a>>::KvRef as TryPartialEq<[u8]>>::Error,
  >
  where
    <Self as GatKvRef<'a>>::KvRef: TryPartialOrd<[u8]>,
  {
    use ext::TrySliceExt;
    use rayon::iter::IndexedParallelIterator;
    use rayon::iter::ParallelIterator;
    use rayon::slice::ParallelSlice;
    let elements = self.elements();
    assert!(!elements.is_empty());
    let elements_start = elements.as_ptr().addr();
    let chunk_size = (elements.len() / rayon::current_num_threads()).min(16);
    let p = elements
      .par_chunks(chunk_size)
      .enumerate()
      .filter_map(|(chunk_index, chunk)| {
        let first = &chunk[0];
        let first_key_start = first.kv_data_start(0);
        let first_key = self.get_ref_slice(first_key_start..first_key_start + first.elem_key_len());
        match TryPartialOrd::try_gt(&first_key, v) {
          Ok(true) => None,
          Ok(false) => {
            let index = chunk.try_binary_search_by(|element| {
              let element_index =
                (ptr::from_ref(element).addr() - elements_start) / size_of::<Self::Element>();
              let key_start = element.kv_data_start(element_index);
              let key = self.get_ref_slice(key_start..key_start + element.elem_key_len());
              Ok(TryPartialOrd::try_partial_cmp(&key, v)?.unwrap())
            });
            Some(Ok((chunk_index, index)))
          }
          Err(report) => Some(Err(report)),
        }
      });
    let (chunk, ord_result) = p
      .try_reduce_with(|(x_chunk, x_result), (y_chunk, y_result)| {
        if x_chunk > y_chunk {
          Ok((x_chunk, x_result))
        } else {
          Ok((y_chunk, y_result))
        }
      })
      .expect("iterator can't be empty")?;
    let result = ord_result?;
    Ok(
      result
        .map(|index| (chunk * chunk_size) + index)
        .map_err(|index| (chunk * chunk_size) + index),
    )
  }
}

#[derive(Clone)]
pub enum NodePage<B, L> {
  Branch(B),
  Leaf(L),
}

impl<'tx, T> TryFrom<TxPage<'tx, T>> for NodePage<BBoltBranch<'tx, T>, BBoltLeaf<'tx, T>>
where
  T: TxPageType<'tx>,
{
  type Error = PageError;

  fn try_from(value: TxPage<'tx, T>) -> Result<Self, Self::Error> {
    if value.page.page_header().is_leaf() {
      Ok(NodePage::Leaf(BBoltLeaf::new(value)))
    } else if value.page.page_header().is_branch() {
      Ok(NodePage::Branch(BBoltBranch::new(value)))
    } else {
      Err(PageError::InvalidNodeFlag(value.page.page_header().flags()))
    }
  }
}

impl<B, L> NodePage<B, L> {
  pub fn is_leaf(&self) -> bool {
    matches!(self, NodePage::Leaf(_))
  }

  pub fn is_branch(&self) -> bool {
    matches!(self, NodePage::Branch(_))
  }
}

impl<B, L> NodePage<B, L>
where
  B: Page,
  L: Page,
{
  pub fn element_count(&self) -> usize {
    let len = match self {
      NodePage::Branch(branch) => branch.page_header().count(),
      NodePage::Leaf(leaf) => leaf.page_header().count(),
    };
    len as usize
  }
}

impl<B, L> Page for NodePage<B, L>
where
  B: Page,
  L: Page,
{
  fn root_page(&self) -> &[u8] {
    match self {
      Self::Branch(page) => page.root_page(),
      Self::Leaf(page) => page.root_page(),
    }
  }
}
