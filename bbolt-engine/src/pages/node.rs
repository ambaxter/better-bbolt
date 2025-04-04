use crate::common::buffer::PageBuffer;
use crate::common::ids::NodePageId;
use crate::common::page::PageHeader;
use bytemuck::{Pod, Zeroable};
use delegate::delegate;
use getset::{CopyGetters, Setters};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::ParallelIterator;
use rayon::slice::ParallelSlice;

#[derive(Clone)]
pub struct NodePage<'tx> {
  page: PageBuffer<'tx>,
}

pub enum NodeType<'p> {
  Branch(BranchPage<'p>),
  Leaf(LeafPage<'p>),
}

impl<'tx> NodePage<'tx> {
  pub fn new(page: PageBuffer<'tx>) -> NodePage<'tx> {
    let header = page.get_header();
    assert!(
      header.is_branch() || header.is_leaf(),
      "Page at {:?} is not a node. Is {:#x}",
      header.id(),
      header.flags()
    );
    if header.is_branch() {
      assert!(
        header.count() > 0,
        "Branch page at {:?} has no entries",
        header.id()
      );
    }
    Self { page }
  }

  #[inline]
  pub fn is_leaf(&self) -> bool {
    self.get_header().is_leaf()
  }

  #[inline]
  pub fn get_count(&self) -> usize {
    self.get_header().count() as usize
  }

  pub fn key(&self, index: usize) -> Option<&[u8]> {
    match self.access() {
      NodeType::Branch(branch) => branch.get_key(index),
      NodeType::Leaf(leaf) => leaf.get_key(index),
    }
  }

  pub fn access(&self) -> NodeType {
    let header = self.get_header();
    if header.is_leaf() {
      let (elements, data) = self.page[size_of::<PageHeader>()..]
        .split_at(header.count() as usize * size_of::<LeafElement>());
      let elements = bytemuck::cast_slice(elements);
      NodeType::Leaf(LeafPage {
        header,
        elements,
        data,
      })
    } else {
      let (elements, data) = self.page[size_of::<PageHeader>()..]
        .split_at(header.count() as usize * size_of::<BranchElement>());
      let elements = bytemuck::cast_slice(elements);
      NodeType::Branch(BranchPage {
        header,
        elements,
        data,
      })
    }
  }

  delegate! {
    to self.page {
      pub fn get_header(&self) -> &PageHeader;
    }
  }
}

pub struct BranchPage<'p> {
  header: &'p PageHeader,
  elements: &'p [BranchElement],
  data: &'p [u8],
}

impl<'p> BranchPage<'p> {
  pub fn get(&self, index: usize) -> Option<NodePageId> {
    self.elements.get(index).map(|element| element.page_id)
  }

  pub fn get_key(&self, index: usize) -> Option<&'p [u8]> {
    self
      .elements
      .get(index)
      .map(|element| unsafe { element.get_key(&self.data) })
  }

  pub fn get_kv(&self, index: usize) -> Option<(&'p [u8], NodePageId)> {
    self.elements.get(index).map(|element| {
      let key = unsafe { element.get_key(&self.data) };
      (key, element.page_id)
    })
  }

  pub fn search(&self, key: &[u8]) -> usize {
    let chunk_size = 16;
    let matching_chunk = self
      .elements
      .par_chunks(chunk_size)
      .enumerate()
      .filter_map(|(chunk_idx, chunk)| {
        if key < unsafe { chunk[0].get_key(&self.data) } {
          None
        } else {
          Some((
            chunk_idx,
            chunk
              .binary_search_by(|element| {
                let element_key = unsafe { element.get_key(&self.data) };
                element_key.cmp(key)
              })
              .unwrap_or_else(|idx| if idx > 0 { idx - 1 } else { idx }),
          ))
        }
      })
      .max_by_key(|matching_chunk| matching_chunk.0)
      .expect(&format!(
        "BranchPage.search - no elements {:?}",
        self.header.id()
      ));
    (chunk_size * matching_chunk.0) + matching_chunk.1
  }
}

pub struct LeafPage<'p> {
  header: &'p PageHeader,
  elements: &'p [LeafElement],
  data: &'p [u8]
}

pub enum LeafValue<'a> {
  Bucket(&'a [u8]),
  Value(&'a [u8]),
}

impl<'p> LeafPage<'p> {
  pub fn get_key(&self, index: usize) -> Option<&'p [u8]> {
    self
      .elements
      .get(index)
      .map(|element| unsafe { element.get_key(&self.data) })
  }
  pub fn get_value(&self, index: usize) -> Option<LeafValue<'p>> {
    self.elements.get(index).map(|element| {
      let bytes = unsafe { element.get_value(&self.data) };
      if element.flags == LeafFlag::BUCKET {
        LeafValue::Bucket(bytes)
      } else {
        LeafValue::Value(bytes)
      }
    })
  }

  pub fn get_kv(&self, index: usize) -> Option<(&'p [u8], LeafValue<'p>)> {
    self.elements.get(index).map(|element| {
      let key = unsafe { element.get_key(&self.data) };
      let value = {
        let bytes = unsafe { element.get_value(&self.data) };
        if element.flags == LeafFlag::BUCKET {
          LeafValue::Bucket(bytes)
        } else {
          LeafValue::Value(bytes)
        }
      };
      (key, value)
    })
  }

  pub fn search(&self, key: &[u8]) -> Option<usize> {
    let chunk_size = 16;
    self
      .elements
      .par_chunks(chunk_size)
      .find_map_first(|chunk| {
        chunk
          .binary_search_by(|element| {
            let element_key = unsafe { element.get_key(&self.data) };
            element_key.cmp(key)
          })
          .ok()
      })
  }

  // TODO: Make multithreaded?
  pub fn partition_point(&self, key: &[u8]) -> usize {
    self.elements.partition_point(|element| {
      let element_key = unsafe { element.get_key(&self.data) };
      element_key < key
    })
  }
}

trait KeyedElement {
  fn key_dist(&self) -> u32;

  fn key_len(&self) -> u32;

  /// Safety: Element must stay within its array on the page
  /// otherwise we'll just wildly read random data
  // Safety requirements as per *const T::offset_from
  unsafe fn get_key_offset(&self, data: &[u8]) -> isize {
    let ele_ptr = self as *const Self as *const u8;
    let dist_to_data = data.as_ptr().offset_from(ele_ptr);
    self.key_dist() as isize - dist_to_data
  }

  /// Safety: Element must stay within its array on the page
  /// otherwise we'll just wildly read random data
  // Safety requirements as per *const T::offset_from
  unsafe fn get_key<'a>(&self, data: &'a [u8]) -> &'a [u8] {
    let key_offset = self.get_key_offset(data) as usize;
    &data[key_offset..key_offset + self.key_len() as usize]
  }
}

trait ValuedElement: KeyedElement {
  fn value_len(&self) -> u32;

  /// Safety: Element must stay within its array on the page
  /// otherwise we'll just wildly read random data
  // Safety requirements as per *const T::offset_from
  unsafe fn get_kv_pair<'a>(&self, data: &'a [u8]) -> (&'a [u8], &'a [u8]) {
    let key_offset = self.get_key_offset(data) as usize;
    let value_offset = key_offset + self.key_len() as usize;
    let key = &data[key_offset..value_offset];
    let value = &data[value_offset..value_offset + self.value_len() as usize];
    (key, value)
  }

  /// Safety: Element must stay within its array on the page
  /// otherwise we'll just wildly read random data
  // Safety requirements as per *const T::offset_from
  unsafe fn get_value<'a>(&self, data: &'a [u8]) -> &'a [u8] {
    let key_offset = self.get_key_offset(data) as usize;
    let value_offset = key_offset + self.key_len() as usize;
    &data[value_offset..value_offset + self.value_len() as usize]
  }
}

///`BranchElement` represents the on-file layout of a branch page's element
///
#[repr(C)]
#[derive(Debug, Copy, Clone, CopyGetters, Setters, Pod, Zeroable)]
pub struct BranchElement {
  #[getset(set = "pub")]
  /// The distance from this element's pointer to its key location
  key_dist: u32,
  #[getset(set = "pub")]
  /// Key length
  key_len: u32,
  /// Page ID of this branch
  page_id: NodePageId,
}

impl BranchElement {
  pub fn new_with_page(page_id: NodePageId) -> BranchElement {
    BranchElement {
      key_dist: 0,
      key_len: 0,
      page_id,
    }
  }
}

impl KeyedElement for BranchElement {
  #[inline]
  fn key_dist(&self) -> u32 {
    self.key_dist
  }

  #[inline]
  fn key_len(&self) -> u32 {
    self.key_len
  }
}

#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Default, bytemuck::Pod, bytemuck::Zeroable)]
pub struct LeafFlag(u32);

bitflags::bitflags! {
  impl LeafFlag: u32 {
    const BUCKET = 0x01;
  }
}

/// `LeafElement` represents the on-file layout of a leaf page's element
///
#[repr(C)]
#[derive(Debug, Copy, Clone, CopyGetters, Setters, Pod, Zeroable)]
pub struct LeafElement {
  #[getset(get_copy = "pub")]
  /// Additional flag for each element. If leaf is a Bucket then 0x01 set
  flags: LeafFlag,
  #[getset(set = "pub")]
  /// The distance from this element's pointer to its key/value location
  key_dist: u32,
  #[getset(set = "pub")]
  /// Key length
  key_len: u32,
  #[getset(set = "pub")]
  /// Value length
  value_len: u32,
}

impl LeafElement {
  pub fn new() -> LeafElement {
    LeafElement {
      flags: Default::default(),
      key_dist: 0,
      key_len: 0,
      value_len: 0,
    }
  }

  pub fn new_bucket() -> LeafElement {
    LeafElement {
      flags: LeafFlag::BUCKET,
      key_dist: 0,
      key_len: 0,
      value_len: 0,
    }
  }
}

impl KeyedElement for LeafElement {
  #[inline]
  fn key_dist(&self) -> u32 {
    self.key_dist
  }

  #[inline]
  fn key_len(&self) -> u32 {
    self.key_len
  }
}

impl ValuedElement for LeafElement {
  #[inline]
  fn value_len(&self) -> u32 {
    self.value_len
  }
}
