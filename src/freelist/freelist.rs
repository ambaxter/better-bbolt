use bbolt_engine::common::bitset::BitSet;
use bbolt_engine::common::ids::{FreePageId, GetPageId, PageId};
use itertools::traits::IteratorIndex;
use std::collections::Bound;
use std::iter::{Enumerate, FlatMap, FusedIterator, Peekable};
use std::mem;
use std::ops::{Index, Range, RangeBounds};
use std::slice::Iter;

pub trait FreelistManager {
  /// Creates a new Freelist Manager
  fn new(freelist: &[FreePageId]) -> Self;

  /// Free a page
  fn free(&mut self, free_page_id: FreePageId);

  /// Assign a free page with `len`
  fn assign(&mut self, parent: PageId, len: usize) -> Option<FreePageId>;

  /// Number of free pages tracked
  fn len(&self) -> usize;

  /// Write out all free pages to an array
  fn write(&self, freelist: &mut [FreePageId]);
}

pub enum LotPage {
  Swap,
  Claimed(usize),
  Freed(usize),
  Array(Box<[u8]>),
}

impl LotPage {
  #[inline]
  pub const fn claimed(page_size: usize) -> LotPage {
    LotPage::Claimed(page_size)
  }

  #[inline]
  pub const fn freed(page_size: usize) -> LotPage {
    LotPage::Freed(page_size)
  }

  pub fn array<T: Into<Box<[u8]>>>(a: T) -> LotPage {
    LotPage::Array(a.into())
  }

  pub fn len(&self) -> usize {
    match self {
      LotPage::Swap => unreachable!(),
      LotPage::Claimed(page_size) => *page_size,
      LotPage::Freed(page_size) => *page_size,
      LotPage::Array(a) => a.len(),
    }
  }

  pub fn is_claimed(&self) -> bool {
    match self {
      LotPage::Swap => unreachable!(),
      LotPage::Claimed(_) => true,
      LotPage::Freed(_) => false,
      LotPage::Array(a) => a.iter().all(|x| *x == 0),
    }
  }

  pub fn is_free(&self) -> bool {
    match self {
      LotPage::Swap => unreachable!(),
      LotPage::Claimed(_) => false,
      LotPage::Freed(_) => true,
      LotPage::Array(a) => a.iter().all(|x| *x != 0),
    }
  }

  pub fn has_free(&self) -> bool {
    match self {
      LotPage::Swap => unreachable!(),
      LotPage::Claimed(_) => false,
      LotPage::Freed(_) => true,
      LotPage::Array(a) => a.iter().any(|x| *x != 0),
    }
  }

  pub fn is_mut(&self) -> bool {
    match self {
      LotPage::Array(_) => true,
      _ => false,
    }
  }

  pub fn get_mut(&mut self) -> &mut [u8] {
    if !self.is_mut() {
      let mut swap = LotPage::Swap;
      mem::swap(self, &mut swap);
      let v = match swap {
        LotPage::Swap => unreachable!(),
        LotPage::Claimed(page_size) => vec![0u8; page_size].into(),
        LotPage::Freed(page_size) => vec![u8::MAX; page_size].into(),
        LotPage::Array(a) => unreachable!(),
      };
      swap = LotPage::Array(v);
      mem::swap(self, &mut swap);
    }
    match self {
      LotPage::Array(a) => a,
      _ => unreachable!(),
    }
  }

  pub fn range<R: RangeBounds<usize>>(&self, lot_page_index: usize, range: R) -> LotPageIter {
    let start = match range.start_bound() {
      Bound::Included(lot) => *lot,
      Bound::Excluded(lot) => *lot + 1,
      Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
      Bound::Included(lot) => *lot + 1,
      Bound::Excluded(lot) => *lot,
      Bound::Unbounded => self.len(),
    };
    let len = self.len();
    assert!(start <= len);
    assert!(end <= len);
    LotPageIter {
      lot_page_index,
      lot_page: &self,
      range: start..end,
    }
  }
}

impl Index<usize> for LotPage {
  type Output = u8;

  fn index(&self, index: usize) -> &Self::Output {
    debug_assert!(index < self.len());
    match self {
      LotPage::Swap => unreachable!(),
      LotPage::Claimed(_) => &0,
      LotPage::Freed(_) => &u8::MAX,
      LotPage::Array(a) => &a[index],
    }
  }
}

#[derive(Clone)]
pub struct LotPageIter<'a> {
  lot_page: &'a LotPage,
  lot_page_index: usize,
  range: Range<usize>,
}

impl<'a> Iterator for LotPageIter<'a> {
  type Item = (usize, u8);

  fn next(&mut self) -> Option<Self::Item> {
    match self.range.next() {
      None => None,
      Some(lot) => Some((self.lot_page_index + lot, self.lot_page[lot])),
    }
  }

  fn size_hint(&self) -> (usize, Option<usize>) {
    let len = self.range.len();
    (len, Some(len))
  }
}

impl<'a> DoubleEndedIterator for LotPageIter<'a> {
  fn next_back(&mut self) -> Option<Self::Item> {
    match self.range.next_back() {
      None => None,
      Some(lot) => Some((self.lot_page_index + lot, self.lot_page[lot])),
    }
  }
}

impl<'a> ExactSizeIterator for LotPageIter<'a> {
  fn len(&self) -> usize {
    self.range.len()
  }
}

impl<'a> FusedIterator for LotPageIter<'a> {}

#[derive(Debug, Copy, Clone)]
pub enum FindResult {
  Needle(u64, u8),
  Pair((u64, u8), (u64, u8)),
  Range((u64, u8), (u64, u8)),
}

impl FindResult {
  pub fn mid_dist_to(&self, lot: u64) -> u64 {
    match self {
      FindResult::Needle(n_lot, _) => n_lot.abs_diff(lot),
      FindResult::Pair((l_lot, _), (r_lot, _)) => l_lot.abs_diff(lot).min(r_lot.abs_diff(lot)),
      FindResult::Range((l_lot, _), (r_lot, _)) => l_lot.abs_diff(lot).min(r_lot.abs_diff(lot)),
    }
  }
}

pub struct FindStore {
  goal_lot: u64,
  result: Option<FindResult>,
}

impl FindStore {
  pub fn new(goal_lot: u64) -> FindStore {
    FindStore {
      goal_lot,
      result: None,
    }
  }

  pub fn is_dist_exceeded(&self, current_lot: u64) -> bool {
    match self.result {
      None => false,
      Some(r) => current_lot.abs_diff(self.goal_lot) > r.mid_dist_to(self.goal_lot),
    }
  }

  pub fn submit_result(&mut self, result: FindResult) {
    match self.result.take() {
      None => self.result = Some(result),
      Some(found) => {
        if found.mid_dist_to(self.goal_lot) < result.mid_dist_to(self.goal_lot) {
          self.result = Some(found);
        } else {
          self.result = Some(result);
        }
      }
    }
  }
}

pub struct FreePageStore {
  page_size: usize,
  store: Vec<LotPage>,
}

impl FreePageStore {
  pub fn new(page_size: usize) -> FreePageStore {
    FreePageStore {
      page_size,
      store: Vec::new(),
    }
  }

  pub fn with_free_pages(page_size: usize, page_count: usize) -> FreePageStore {
    let mut store = Vec::new();
    for _ in 0..page_count {
      store.push(LotPage::Freed(page_size));
    }
    FreePageStore { page_size, store }
  }

  pub fn with_claimed_pages(page_size: usize, page_count: usize) -> FreePageStore {
    let mut store = Vec::new();
    for _ in 0..page_count {
      store.push(LotPage::Claimed(page_size));
    }
    FreePageStore { page_size, store }
  }

  pub fn with_free_page_ids(page_size: usize, page_ids: &[FreePageId]) -> FreePageStore {
    let mut store = FreePageStore::new(page_size);
    page_ids.iter().for_each(|page_id| store.free(*page_id));
    store
  }

  fn get_location<T: Into<PageId>>(&self, page_id: T) -> (usize, usize, u8) {
    let id = page_id.into().0;
    let store_lot = id / 8;
    let offset = (id % 8) as u8;
    let store_index = (store_lot / self.page_size as u64) as usize;
    let lot_index = (store_lot % self.page_size as u64) as usize;
    (store_index, lot_index, offset)
  }

  fn get_location_usize(&self, store_lot: usize) -> (usize, usize) {
    let store_index = store_lot / self.page_size;
    let lot_index = store_lot % self.page_size;
    (store_index, lot_index)
  }

  // TODO: Handle len/overflow
  pub fn free<T: Into<FreePageId>>(&mut self, page_id: T) {
    let (store_index, lot_index, offset) = self.get_location(page_id.into());
    assert!(store_index < self.store.len());
    self.store.as_mut_slice()[store_index].get_mut()[lot_index].set(offset);
  }

  pub fn claim<T: Into<PageId>>(&mut self, page_id: T) {
    let (store_index, lot_index, offset) = self.get_location(page_id);
    assert!(store_index < self.store.len());
    self.store.as_mut_slice()[store_index].get_mut()[lot_index].unset(offset);
  }

  pub fn len(&self) -> usize {
    self.store.len() * self.page_size * 8
  }

  pub fn find_near<T: Into<PageId>>(&self, page_id: T, len: usize) -> Option<FreePageId> {
    let page_id = page_id.into();
    let mut result = FindStore::new(page_id.0);
    assert_ne!(len, 0);
    match (len / 8, (len % 8) as u8) {
      (0, n) => match n {
        1 => {
          unimplemented!()
        }
        2 => unimplemented!(),
        3 => unimplemented!(),
        4 => unimplemented!(),
        5 => unimplemented!(),
        6 => unimplemented!(),
        7 => unimplemented!(),
        _ => unreachable!(),
      },
      (m, n) => unimplemented!(),
    }
  }
  fn range<'a, R: RangeBounds<usize>>(
    &'a self, range: R,
  ) -> FreePageRangeIter<'a, impl FnMut((usize, &'a LotPage)) -> LotPageIter<'a> + 'a> {
    let (store_index_start, lot_index_start) = match range.start_bound() {
      Bound::Included(store_lot_start) => {
        let store_index_start = store_lot_start / self.page_size;
        let lot_index_start = store_lot_start % self.page_size;
        (store_index_start, lot_index_start)
      }
      Bound::Excluded(store_lot_start) => {
        let mut lot_index_start = store_lot_start / self.page_size;
        let mut store_index_start = store_lot_start % self.page_size;
        if lot_index_start + 1 == self.page_size {
          store_index_start += 1;
          lot_index_start = 0;
        } else {
          lot_index_start += 1;
        }
        (store_index_start, lot_index_start)
      }
      Bound::Unbounded => (0, 0),
    };
    let (store_index_end, lot_index_end) = match range.end_bound() {
      Bound::Included(store_lot_end) => {
        let mut store_index_end = store_lot_end / self.page_size;
        let mut lot_index_end = store_lot_end % self.page_size;
        if lot_index_end + 1 == self.page_size {
          store_index_end += 1;
          lot_index_end = 0;
        } else {
          lot_index_end += 1;
        }
        (store_index_end, lot_index_end)
      }
      Bound::Excluded(store_lot_end) => {
        let store_index_end = store_lot_end / self.page_size;
        let lot_index_end = store_lot_end % self.page_size;
        (store_index_end, lot_index_end)
      }
      Bound::Unbounded => (self.store.len(), 0),
    };

    let page_size = self.page_size;

    let f = move |(store_index, lot): (usize, &'a LotPage)| match (
      store_index == store_index_start,
      store_index == store_index_end,
    ) {
      (true, true) => lot.range(store_index * page_size, lot_index_start..lot_index_end),
      (true, false) => lot.range(store_index * page_size, lot_index_start..),
      (false, true) => lot.range(store_index * page_size, ..lot_index_end),
      (false, false) => lot.range(store_index * page_size, ..),
    };

    let len = self.store[store_index_start..store_index_end + 1]
      .iter()
      .enumerate()
      .map(f)
      .map(|i| i.len())
      .sum();

    let r = self.store[store_index_start..store_index_end + 1]
      .iter()
      .enumerate()
      .flat_map(f);
    FreePageRangeIter { r, len }
  }
}

struct LotStoreIndex<'a> {
  store: &'a FreePageStore,
}

impl<'a> LotStoreIndex<'a> {
  fn len(&self) -> usize {
    self.store.store.len() * self.store.page_size
  }

  fn lot_for_page<T: Into<PageId>>(&self, page_id: T) -> usize {
    let page_id = page_id.into();
    (page_id.0 / 8) as usize
  }
}

impl<'a> Index<usize> for LotStoreIndex<'a> {
  type Output = u8;
  fn index(&self, index: usize) -> &u8 {
    let (store_index, lot_index) = self.store.get_location_usize(index);
    assert!(store_index < self.store.store.len());
    &self.store.store[store_index][lot_index]
  }
}

#[derive(Clone)]
struct FreePageRangeIter<'a, F: FnMut((usize, &'a LotPage)) -> LotPageIter<'a> + 'a> {
  r: FlatMap<Enumerate<Iter<'a, LotPage>>, LotPageIter<'a>, F>,
  len: usize,
}

impl<'a, F: FnMut((usize, &'a LotPage)) -> LotPageIter<'a> + 'a> Iterator
  for FreePageRangeIter<'a, F>
{
  type Item = (usize, u8);

  fn next(&mut self) -> Option<Self::Item> {
    self.r.next().inspect(|_| self.len -= 1)
  }
}

impl<'a, F: FnMut((usize, &'a LotPage)) -> LotPageIter<'a> + 'a> DoubleEndedIterator
  for FreePageRangeIter<'a, F>
{
  fn next_back(&mut self) -> Option<Self::Item> {
    self.r.next_back().inspect(|_| self.len -= 1)
  }
}

impl<'a, F: FnMut((usize, &'a LotPage)) -> LotPageIter<'a> + 'a> ExactSizeIterator
  for FreePageRangeIter<'a, F>
{
  fn len(&self) -> usize {
    self.len
  }
}

impl<'a, F: FnMut((usize, &'a LotPage)) -> LotPageIter<'a> + 'a> FusedIterator
  for FreePageRangeIter<'a, F>
{
}

impl<'a, F: FnMut((usize, &'a LotPage)) -> LotPageIter<'a>> FreePageRangeIter<'a, F> {}
