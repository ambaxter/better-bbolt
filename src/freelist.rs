use bbolt_engine::common::ids::{FreePageId, GetPageId, PageId};
use itertools::izip;

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

pub mod search {
  use itertools::izip;

  pub struct Needle<const N: usize> {
    masks: [u8; N],
  }

  impl<const N: usize> Needle<N> {
    pub const fn new(masks: [u8; N]) -> Needle<N> {
      Needle { masks }
    }

    #[inline]
    pub fn masks(&self) -> &[u8; N] {
      &self.masks
    }

    pub fn eq_mask<'a>(&'a self) -> impl Fn(u8) -> bool + 'a {
      |byte| self.masks.iter().any(|mask| (byte & mask) == *mask)
    }

    pub fn any_bits<'a>(&'a self) -> impl Fn(u8) -> bool + 'a {
      |byte| self.masks.iter().copied().any(|mask| (byte & mask) != 0)
    }
  }

  pub const N2: Needle<7> = Needle::new([
    0b1100_0000u8,
    0b0110_0000u8,
    0b0011_0000u8,
    0b0001_1000u8,
    0b0000_1100u8,
    0b0000_0110u8,
    0b0000_0011u8,
  ]);

  pub const N3: Needle<6> = Needle::new([
    0b1110_0000u8,
    0b0111_0000u8,
    0b0011_1000u8,
    0b0001_1100u8,
    0b0000_1110u8,
    0b0000_0111u8,
  ]);

  pub const N4: Needle<5> = Needle::new([
    0b1111_0000u8,
    0b0111_1000u8,
    0b0011_1100u8,
    0b0001_1110u8,
    0b0000_1111u8,
  ]);

  pub const N5: Needle<4> =
    Needle::new([0b1111_1000u8, 0b0111_1100u8, 0b0011_1110u8, 0b0001_1111u8]);
  pub const N6: Needle<3> = Needle::new([0b1111_1100u8, 0b0111_1110u8, 0b0011_1111u8]);

  // I’ve killed worse than you on my way to real problems - Commander Shepard
  pub const N7: Needle<2> = Needle::new([0b1111_1110u8, 0b0111_1111u8]);

  pub const N8: Needle<1> = Needle::new([0b1111_1111u8]);

  pub struct SingleEnded {
    left: u8,
    right: u8,
  }

  impl SingleEnded {
    pub const fn new(left: u8, right: u8) -> SingleEnded {
      SingleEnded { left, right }
    }

    #[inline]
    pub fn left(&self) -> u8 {
      self.left
    }

    #[inline]
    pub fn right(&self) -> u8 {
      self.right
    }
  }

  pub const SE1: SingleEnded = SingleEnded::new(0b1000_0000u8, 0b0000_0001u8);
  pub const SE2: SingleEnded = SingleEnded::new(0b1100_0000u8, 0b0000_0011u8);
  pub const SE3: SingleEnded = SingleEnded::new(0b1110_0000u8, 0b0000_0111u8);
  pub const SE4: SingleEnded = SingleEnded::new(0b1111_0000u8, 0b0000_1111u8);
  pub const SE5: SingleEnded = SingleEnded::new(0b1111_1000u8, 0b0001_1111u8);
  pub const SE6: SingleEnded = SingleEnded::new(0b1111_1100u8, 0b0011_1111u8);
  pub const SE7: SingleEnded = SingleEnded::new(0b1111_1110u8, 0b0111_1111u8);

  pub struct DoubleEnded<const N: usize> {
    masks: [(u8, u8); N],
  }

  impl<const N: usize> DoubleEnded<N> {
    pub const fn new(masks: [(u8, u8); N]) -> DoubleEnded<N> {
      DoubleEnded { masks }
    }

    pub fn masks(&self) -> &[(u8, u8); N] {
      &self.masks
    }

    pub fn eq_mask<'a>(&'a self) -> impl Fn(u8, u8) -> bool + 'a {
      |l_byte, r_byte| {
        self
          .masks
          .iter()
          .copied()
          .any(|(l_mask, r_mask)| (l_byte & l_mask) == l_mask && (r_byte & r_mask) == r_mask)
      }
    }
  }

  pub const DE2: DoubleEnded<1> = DoubleEnded::new([(0b1000_0000u8, 0b0000_0001u8)]);
  pub const DE3: DoubleEnded<2> = DoubleEnded::new([
    (0b1100_0000u8, 0b0000_0001u8),
    (0b1000_0000u8, 0b0000_0011u8),
  ]);

  pub const DE4: DoubleEnded<3> = DoubleEnded::new([
    (0b1110_0000u8, 0b0000_0001u8),
    (0b1100_0000u8, 0b0000_0011u8),
    (0b1000_0000u8, 0b0000_0111u8),
  ]);

  pub const DE5: DoubleEnded<4> = DoubleEnded::new([
    (0b1111_0000u8, 0b0000_0001u8),
    (0b1110_0000u8, 0b0000_0011u8),
    (0b1100_0000u8, 0b0000_0111u8),
    (0b1000_0000u8, 0b0000_1111u8),
  ]);

  pub const DE6: DoubleEnded<5> = DoubleEnded::new([
    (0b1111_1000u8, 0b0000_0001u8),
    (0b1111_0000u8, 0b0000_0011u8),
    (0b1110_0000u8, 0b0000_0111u8),
    (0b1100_0000u8, 0b0000_1111u8),
    (0b1000_0000u8, 0b0001_1111u8),
  ]);

  pub const DE7: DoubleEnded<6> = DoubleEnded::new([
    (0b1111_1100u8, 0b0000_0001u8),
    (0b1111_1000u8, 0b0000_0011u8),
    (0b1111_0000u8, 0b0000_0111u8),
    (0b1110_0000u8, 0b0000_1111u8),
    (0b1100_0000u8, 0b0001_1111u8),
    (0b1000_0000u8, 0b0011_1111u8),
  ]);

  pub const DE8: DoubleEnded<7> = DoubleEnded::new([
    (0b1111_1110u8, 0b0000_0001u8),
    (0b1111_1100u8, 0b0000_0011u8),
    (0b1111_1000u8, 0b0000_0111u8),
    (0b1111_0000u8, 0b0000_1111u8),
    (0b1110_0000u8, 0b0001_1111u8),
    (0b1100_0000u8, 0b0011_1111u8),
    (0b1000_0000u8, 0b0111_1111u8),
  ]);

  #[cfg(test)]
  mod tests {
    use crate::freelist::search::*;

    #[test]
    fn count_needles() {
      for mask in N2.masks().iter() {
        assert_eq!(mask.count_ones(), 2)
      }
      for mask in N3.masks().iter() {
        assert_eq!(mask.count_ones(), 3)
      }
      for mask in N4.masks().iter() {
        assert_eq!(mask.count_ones(), 4)
      }
      for mask in N5.masks().iter() {
        assert_eq!(mask.count_ones(), 5)
      }
      for mask in N6.masks().iter() {
        assert_eq!(mask.count_ones(), 6)
      }
      for mask in N7.masks().iter() {
        assert_eq!(mask.count_ones(), 7)
      }
      for mask in N8.masks().iter() {
        assert_eq!(mask.count_ones(), 8)
      }
    }

    #[test]
    fn count_single_ended() {
      assert_eq!(1, SE1.left().count_ones());
      assert_eq!(1, SE1.right().count_ones());
      assert_eq!(2, SE2.left().count_ones());
      assert_eq!(2, SE2.right().count_ones());
      assert_eq!(3, SE3.left().count_ones());
      assert_eq!(3, SE3.right().count_ones());
      assert_eq!(4, SE4.left().count_ones());
      assert_eq!(4, SE4.right().count_ones());
      assert_eq!(5, SE5.left().count_ones());
      assert_eq!(5, SE5.right().count_ones());
      assert_eq!(6, SE6.left().count_ones());
      assert_eq!(6, SE6.right().count_ones());
      assert_eq!(7, SE7.left().count_ones());
      assert_eq!(7, SE7.right().count_ones());
    }

    #[test]
    fn count_double_ended() {
      DE3
        .masks()
        .iter()
        .all(|(l_mask, r_mask)| (l_mask.count_ones() + r_mask.count_ones()) == 3);
      DE4
        .masks()
        .iter()
        .all(|(l_mask, r_mask)| (l_mask.count_ones() + r_mask.count_ones()) == 4);
      DE5
        .masks()
        .iter()
        .all(|(l_mask, r_mask)| (l_mask.count_ones() + r_mask.count_ones()) == 5);
      DE6
        .masks()
        .iter()
        .all(|(l_mask, r_mask)| (l_mask.count_ones() + r_mask.count_ones()) == 6);
      DE7
        .masks()
        .iter()
        .all(|(l_mask, r_mask)| (l_mask.count_ones() + r_mask.count_ones()) == 7);
      DE8
        .masks()
        .iter()
        .all(|(l_mask, r_mask)| (l_mask.count_ones() + r_mask.count_ones()) == 8);
    }

    #[test]
    fn needle_all_or_none() {
      let all = 0b1111_1111u8;
      let none = 0u8;
      assert!(N2.eq_mask()(all));
      assert!(!N2.eq_mask()(none));
      assert!(N3.eq_mask()(all));
      assert!(!N3.eq_mask()(none));
      assert!(N4.eq_mask()(all));
      assert!(!N4.eq_mask()(none));
      assert!(N5.eq_mask()(all));
      assert!(!N5.eq_mask()(none));
      assert!(N6.eq_mask()(all));
      assert!(!N6.eq_mask()(none));
      assert!(N7.eq_mask()(all));
      assert!(!N7.eq_mask()(none));
      assert!(N8.eq_mask()(all));
      assert!(!N8.eq_mask()(none));
    }
  }
}

#[cfg(test)]
mod test {
  use crate::freelist::search::{DoubleEnded, DE2, N2, N8};
  use crate::freelist::FreelistManager;
  use bbolt_engine::common::bitset::BitSet;
  use bbolt_engine::common::ids::{FreePageId, PageId};
  use itertools::Itertools;
  use std::collections::{btree_map, BTreeMap, Bound};
  use std::iter::{FlatMap, FusedIterator};
  use std::marker::PhantomData;
  use std::mem;
  use std::ops::{Index, Range, RangeBounds};

  #[derive(Debug, Copy, Clone)]
  pub enum FindResult {
    Needle(usize, u8),
    Pair((usize, u8), (usize, u8)),
    Range((usize, u8), (usize, u8)),
  }

  impl FindResult {
    pub fn mid_dist_to(&self, lot: usize) -> usize {
      match self {
        FindResult::Needle(n_lot, _) => n_lot.abs_diff(lot),
        FindResult::Pair((l_lot, _), (r_lot, _)) => l_lot.abs_diff(lot).min(r_lot.abs_diff(lot)),
        FindResult::Range((l_lot, _), (r_lot, _)) => l_lot.abs_diff(lot).min(r_lot.abs_diff(lot)),
      }
    }
  }

  pub struct FindStore {
    partition: usize,
    result: Option<FindResult>,
  }

  impl FindStore {
    pub fn new(partition: usize) -> FindStore {
      FindStore {
        partition,
        result: None,
      }
    }

    pub fn is_dist_exceeded(&self, current_lot: usize) -> bool {
      match self.result {
        None => false,
        Some(r) => current_lot.abs_diff(self.partition) > r.mid_dist_to(self.partition),
      }
    }

    pub fn submit_result(&mut self, result: FindResult) {
      match self.result.take() {
        None => self.result = Some(result),
        Some(found) => {
          if found.mid_dist_to(self.partition) < result.mid_dist_to(self.partition) {
            self.result = Some(found);
          } else {
            self.result = Some(result);
          }
        }
      }
    }
  }

  #[test]
  fn test_find_needle() {
    let v = [
      0u8,
      0,
      0b0000_0001u8,
      0b1000_0000u8,
      0b1000_0000u8,
      0,
      0,
      0,
      0,
      0,
      10,
    ];
    for (a, b) in v
      .iter()
      .enumerate()
      .filter(|(_, byte)| N8.any_bits()(**byte))
    {
      println!("{:?} {:?}", a, b);
    }

    for (a, b) in v
      .iter()
      .enumerate()
      .rev()
      .filter(|(_, byte)| N8.any_bits()(**byte))
    {
      println!("{:?} {:?}", a, b);
    }
  }

  #[test]
  fn test_setting() {
    let mut v = vec![0u8; 8];
    let ids = [3usize, 4, 7, 9];
    for id in ids {
      let (lot, bit) = (id / 8, (id % 8) as u8);
      v[lot].set(bit);
    }
    for lot in v {
      println!("{:#010b}", lot);
    }
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

  pub struct FreePageStore {
    page_size: usize,
    store: BTreeMap<usize, LotPage>,
  }

  impl FreePageStore {
    pub fn new(page_size: usize) -> FreePageStore {
      FreePageStore {
        page_size,
        store: BTreeMap::new(),
      }
    }

    pub fn with_free_pages(page_size: usize, page_count: usize) -> FreePageStore {
      let mut store = BTreeMap::new();
      for i in 0..page_count {
        store.insert(i, LotPage::Freed(page_size));
      }
      FreePageStore { page_size, store }
    }

    pub fn with_claimed_pages(page_size: usize, page_count: usize) -> FreePageStore {
      let mut store = BTreeMap::new();
      for i in 0..page_count {
        store.insert(i, LotPage::Claimed(page_size));
      }
      FreePageStore { page_size, store }
    }

    pub fn with_free_page_ids(page_size: usize, page_ids: &[FreePageId]) -> FreePageStore {
      let mut store = FreePageStore::new(page_size);
      page_ids.iter().for_each(|page_id| store.free(*page_id));
      store
    }

    pub fn get_location<T: Into<PageId>>(&self, page_id: T) -> (usize, usize, u8) {
      let id = page_id.into().0;
      let store_lot = id / 8;
      let offset = (id % 8) as u8;
      let store_index = (store_lot / self.page_size as u64) as usize;
      let lot_index = (store_lot % self.page_size as u64) as usize;
      (store_index, lot_index, offset)
    }

    // TODO: Handle len/overflow
    pub fn free<T: Into<FreePageId>>(&mut self, page_id: T) {
      let (store_index, lot_index, offset) = self.get_location(page_id.into());
      self
        .store
        .entry(store_index)
        .or_insert(LotPage::Freed(self.page_size))
        .get_mut()[lot_index]
        .set(offset);
    }

    pub fn claim<T: Into<PageId>>(&mut self, page_id: T) {
      let (store_index, lot_index, offset) = self.get_location(page_id);
      self
        .store
        .entry(store_index)
        .or_insert(LotPage::Freed(self.page_size))
        .get_mut()[lot_index]
        .unset(offset);
    }

    pub fn find_near<T: Into<PageId>>(&self, page_id: T, len: usize) -> Option<FreePageId> {
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
    ) -> FreePageRangeIter<'a, impl FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> + 'a> {
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
        Bound::Unbounded => {
          if let Some((store_index_end, _)) = self.store.last_key_value() {
            (*store_index_end + 1, 0)
          } else {
            (0, 0)
          }
        }
      };

      let page_size = self.page_size;

      let f = move |(store_index, lot): (&'a usize, &'a LotPage)| match (
        *store_index == store_index_start,
        *store_index == store_index_end,
      ) {
        (true, true) => lot.range(*store_index * page_size, lot_index_start..lot_index_end),
        (true, false) => lot.range(*store_index * page_size, lot_index_start..),
        (false, true) => lot.range(*store_index * page_size, ..lot_index_end),
        (false, false) => lot.range(*store_index * page_size, ..),
      };

      let len = self
        .store
        .range(store_index_start..store_index_end + 1)
        .map(f)
        .map(|i| i.len())
        .sum();

      let r = self
        .store
        .range(store_index_start..store_index_end + 1)
        .flat_map(f);
      FreePageRangeIter { r, len }
    }
  }

  #[derive(Clone)]
  struct FreePageRangeIter<'a, F: FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> + 'a> {
    r: FlatMap<btree_map::Range<'a, usize, LotPage>, LotPageIter<'a>, F>,
    len: usize,
  }

  impl<'a, F: FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> + 'a> Iterator
    for FreePageRangeIter<'a, F>
  {
    type Item = (usize, u8);

    fn next(&mut self) -> Option<Self::Item> {
      self.r.next().inspect(|_| self.len -= 1)
    }
  }

  impl<'a, F: FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> + 'a> DoubleEndedIterator
    for FreePageRangeIter<'a, F>
  {
    fn next_back(&mut self) -> Option<Self::Item> {
      self.r.next_back().inspect(|_| self.len -= 1)
    }
  }

  impl<'a, F: FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> + 'a> ExactSizeIterator
    for FreePageRangeIter<'a, F>
  {
    fn len(&self) -> usize {
      self.len
    }
  }

  impl<'a, F: FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> + 'a> FusedIterator
    for FreePageRangeIter<'a, F>
  {
  }

  impl<'a, F> FreePageRangeIter<'a, F> where F: FnMut((&'a usize, &'a LotPage)) -> LotPageIter<'a> {}

  #[test]
  pub fn near() {
    for i in 1..33u8 {
      let div = i / 8;
      let rem = i % 8;
      println!("{:?}: {:?} - {:?}", i, div, rem);
    }
  }

  #[test]
  pub fn test_page_iter() {
    let store = FreePageStore::with_free_pages(4096, 4);
    for i in store.range(0..160000) {
      println!("{:?}", i);
    }
  }

  #[test]
  pub fn search_page_single() {
    let mut store = FreePageStore::with_claimed_pages(4096, 1);
    store.free(FreePageId::of(159));
    store.free(FreePageId::of(160));

    if let Some(i) = &store
      .range(0..400)
      .rev()
      .filter(|(_, byte)| N8.any_bits()(*byte))
      .next()
    {
      println!("{:?}", i);
    };

    if let Some(i) = &store
      .range(0..400)
      .filter(|(_, byte)| N8.any_bits()(*byte))
      .next()
    {
      println!("{:?}", i);
    };
  }

  #[test]
  pub fn search_page_two() {
    let mut store = FreePageStore::with_claimed_pages(4096, 1);
    store.free(FreePageId::of(159));
    store.free(FreePageId::of(160));

    if let Some(i) = &store
      .range(0..400)
      .filter(|(_, byte)| N8.eq_mask()(*byte))
      .next()
    {
      println!("{:?}", i);
    };

    if let Some(i) = &store
      .range(0..400)
      .rev()
      .filter(|(_, byte)| N2.eq_mask()(*byte))
      .next()
    {
      println!("{:?}", i);
    };

    if let Some(i) = store
      .range(0..400)
      .rev()
      .tuple_windows()
      .map(|(r, l)| (l, r))
      .filter(|((_, l), (_, r))| DE2.eq_mask()(*l, *r))
      .next()
    {
      println!("{:?}", i);
    };
  }
}
