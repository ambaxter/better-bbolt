use crate::common::errors::{CursorError, OpsError, PageError};
use crate::common::id::NodePageId;
use crate::common::layout::node::{BranchElement, LeafElement, LeafFlag};
use crate::common::layout::page::PageHeader;
use crate::io::pages::lazy::ops::{TryPartialEq, TryPartialOrd};
use crate::io::pages::types::node::branch::BranchPage;
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::{GatRefKv, GetGatKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use bytemuck::{Pod, cast_slice};
use error_stack::ResultExt;
use std::cmp::Ordering;
use std::cmp::Ordering::{Equal, Greater, Less};
use std::ops::Range;
use std::{hint, ptr};

pub trait TrySliceExt<T> {
  fn try_binary_search_by<'a, F, E>(&'a self, f: F) -> crate::Result<Result<usize, usize>, E>
  where
    F: FnMut(&'a T) -> crate::Result<Ordering, E>,
    T: 'a;
}

impl<T> TrySliceExt<T> for [T] {
  fn try_binary_search_by<'a, F, E>(&'a self, mut f: F) -> crate::Result<Result<usize, usize>, E>
  where
    F: FnMut(&'a T) -> crate::Result<Ordering, E>,
    T: 'a,
  {
    let mut size = self.len();
    if size == 0 {
      return Ok(Err(0));
    }
    let mut base = 0usize;

    // This loop intentionally doesn't have an early exit if the comparison
    // returns Equal. We want the number of loop iterations to depend *only*
    // on the size of the input slice so that the CPU can reliably predict
    // the loop count.
    while size > 1 {
      let half = size / 2;
      let mid = base + half;

      // SAFETY: the call is made safe by the following inconstants:
      // - `mid >= 0`: by definition
      // - `mid < size`: `mid = size / 2 + size / 4 + size / 8 ...`
      let cmp = f(unsafe { self.get_unchecked(mid) })?;

      // Binary search interacts poorly with branch prediction, so force
      // the compiler to use conditional moves if supported by the target
      // architecture.
      // TODO: select_unpredictable is unstable so I can't use it here, yet
      // Hopefully, soooooon!
      // https://github.com/rust-lang/rust/issues/133962
      //base = (cmp == Greater).select_unpredictable(base, mid);
      base = if cmp == Greater { base } else { mid };

      // This is imprecise in the case where `size` is odd and the
      // comparison returns Greater: the mid element still gets included
      // by `size` even though it's known to be larger than the element
      // being searched for.
      //
      // This is fine though: we gain more performance by keeping the
      // loop iteration count invariant (and thus predictable) than we
      // lose from considering one additional element.
      size -= half;
    }

    // SAFETY: base is always in [0, size) because base <= mid.
    let cmp = f(unsafe { self.get_unchecked(base) })?;
    if cmp == Equal {
      // SAFETY: same as the `get_unchecked` above.
      unsafe { hint::assert_unchecked(base < self.len()) };
      Ok(Ok(base))
    } else {
      let result = base + (cmp == Less) as usize;
      // SAFETY: same as the `get_unchecked` above.
      // Note that this is `<=`, unlike the assume in the `Ok` path.
      unsafe { hint::assert_unchecked(result <= self.len()) };
      Ok(Err(result))
    }
  }
}

pub mod branch;
pub mod leaf;

pub trait HasKeyRefs: GetGatKvRefSlice {
  fn key_ref<'a>(&'a self, index: usize) -> Option<<Self as GatRefKv<'a>>::RefKv>;
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
    <Self as GatRefKv<'a>>::RefKv: PartialOrd<[u8]>,
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
    <<Self as GatRefKv<'a>>::RefKv as TryPartialEq<[u8]>>::Error,
  >
  where
    <Self as GatRefKv<'a>>::RefKv: TryPartialOrd<[u8]>,
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
    <Self as GatRefKv<'a>>::RefKv: PartialOrd<[u8]>,
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
    <<Self as GatRefKv<'a>>::RefKv as TryPartialEq<[u8]>>::Error,
  >
  where
    <Self as GatRefKv<'a>>::RefKv: TryPartialOrd<[u8]>,
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

pub trait HasNodes<'tx>: HasKeys<'tx> {
  fn node(&self, index: usize) -> Option<NodePageId>;
}

pub trait HasValues<'tx>: HasKeys<'tx> {
  fn leaf_flag(&self, index: usize) -> Option<LeafFlag>;

  fn value_ref<'a>(&'a self, index: usize) -> Option<<Self as GatRefKv<'a>>::RefKv>;

  fn key_value_ref<'a>(
    &'a self, index: usize,
  ) -> Option<(<Self as GatRefKv<'a>>::RefKv, <Self as GatRefKv<'a>>::RefKv)>;

  fn value(&self, index: usize) -> Option<Self::TxKv>;

  fn key_value(&self, index: usize) -> Option<(Self::TxKv, Self::TxKv)>;
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
  pub fn element_count(&self) -> usize {
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
