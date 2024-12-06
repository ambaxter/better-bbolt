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

    pub fn eq_mask<'a>(&'a self) -> impl Fn(&u8) -> bool + 'a {
      |byte| self.masks.iter().any(|mask| (*byte & mask) == *mask)
    }

    pub fn any_mask<'a>(&'a self) -> impl Fn(&u8) -> bool + 'a {
      |byte| self.masks.iter().any(|mask| (*byte & mask) != 0)
    }
  }

  pub struct Spread<const M: usize> {
    left_masks: [u8; M],
    right_masks: [u8; M],
  }

  impl<const M: usize> Spread<M> {
    pub const fn new(left_masks: [u8; M], right_masks: [u8; M]) -> Spread<M> {
      Spread {
        left_masks,
        right_masks,
      }
    }

    #[inline]
    pub fn left_masks(&self) -> &[u8; M] {
      &self.left_masks
    }

    #[inline]
    pub fn right_masks(&self) -> &[u8; M] {
      &self.right_masks
    }

    pub fn eq_mask<'a>(&'a self) -> impl Fn(&u8, &u8) -> bool + 'a {
      |l_byte, r_byte| {
        izip!(&self.left_masks, &self.right_masks).any(|(l_mask, r_mask)| {
          ((*l_byte & l_mask) == *l_mask) && ((*r_byte & r_mask) == *r_mask)
        })
      }
    }
  }

  pub static NEEDLE_02: Needle<7> = Needle::new([
    0b1100_0000u8,
    0b0110_0000u8,
    0b0011_0000u8,
    0b0001_1000u8,
    0b0000_1100u8,
    0b0000_0110u8,
    0b0000_0011u8,
  ]);

  pub static SPREAD_02: Spread<1> = Spread::new([0b0000_0001u8], [0b1000_0000u8]);

  pub static NEEDLE_03: Needle<6> = Needle::new([
    0b1110_0000u8,
    0b0111_0000u8,
    0b0011_1000u8,
    0b0001_1100u8,
    0b0000_1110u8,
    0b0000_0111u8,
  ]);

  pub static SPREAD_03: Spread<2> = Spread::new(
    [0b0000_011u8, 0b0000_0001u8],
    [0b1000_0000u8, 0b1100_0000u8],
  );

  pub static NEEDLE_04: Needle<5> = Needle::new([
    0b1111_0000u8,
    0b0111_1000u8,
    0b0011_1100u8,
    0b0001_1110u8,
    0b0000_1111u8,
  ]);

  pub static SPREAD_04: Spread<3> = Spread::new(
    [0b0000_0111u8, 0b0000_0011u8, 0b0000_0001u8],
    [0b1000_0000u8, 0b1100_0000u8, 0b1110_0000u8],
  );

  pub static NEEDLE_05: Needle<4> =
    Needle::new([0b1111_1000u8, 0b0111_1100u8, 0b0011_1110u8, 0b0001_1111u8]);

  pub static SPREAD_05: Spread<4> = Spread::new(
    [0b0000_1111u8, 0b0000_0111u8, 0b0000_0011u8, 0b0000_0001u8],
    [0b1000_0000u8, 0b1100_0000u8, 0b1110_0000u8, 0b1111_0000u8],
  );

  pub static NEEDLE_06: Needle<3> = Needle::new([0b1111_1100u8, 0b0111_1110u8, 0b0011_1111u8]);

  pub static SPREAD_06: Spread<5> = Spread::new(
    [
      0b0001_1111u8,
      0b0000_1111u8,
      0b0000_0111u8,
      0b0000_0011u8,
      0b0000_0001u8,
    ],
    [
      0b1000_0000u8,
      0b1100_0000u8,
      0b1110_0000u8,
      0b1111_0000u8,
      0b1111_1000u8,
    ],
  );

  pub static NEEDLE_07: Needle<2> = Needle::new([0b1111_1110u8, 0b0111_1111u8]);

  pub static SPREAD_07: Spread<6> = Spread::new(
    [
      0b0011_1111u8,
      0b0001_1111u8,
      0b0000_1111u8,
      0b0000_0111u8,
      0b0000_0011u8,
      0b0000_0001u8,
    ],
    [
      0b1000_0000u8,
      0b1100_0000u8,
      0b1110_0000u8,
      0b1111_0000u8,
      0b1111_1000u8,
      0b1111_1100u8,
    ],
  );

  pub static NEEDLE_08: Needle<1> = Needle::new([0b1111_1111u8]);

  pub static SPREAD_08: Spread<7> = Spread::new(
    [
      0b0111_1111u8,
      0b0011_1111u8,
      0b0001_1111u8,
      0b0000_1111u8,
      0b0000_0111u8,
      0b0000_0011u8,
      0b0000_0001u8,
    ],
    [
      0b1000_0000u8,
      0b1100_0000u8,
      0b1110_0000u8,
      0b1111_0000u8,
      0b1111_1000u8,
      0b1111_1100u8,
      0b1111_1110u8,
    ],
  );

  pub static SPREAD_09: Spread<8> = Spread::new(
    [
      0b1111_1111u8,
      0b0111_1111u8,
      0b0011_1111u8,
      0b0001_1111u8,
      0b0000_1111u8,
      0b0000_0111u8,
      0b0000_0011u8,
      0b0000_0001u8,
    ],
    [
      0b1000_0000u8,
      0b1100_0000u8,
      0b1110_0000u8,
      0b1111_0000u8,
      0b1111_1000u8,
      0b1111_1100u8,
      0b1111_1110u8,
      0b1111_1111u8,
    ],
  );

  // Spread 10 can also be 8 + spread 2

  pub static SPREAD_10: Spread<7> = Spread::new(
    [
      0b1111_1111u8,
      0b0111_1111u8,
      0b0011_1111u8,
      0b0001_1111u8,
      0b0000_1111u8,
      0b0000_0111u8,
      0b0000_0011u8,
    ],
    [
      0b1100_0000u8,
      0b1110_0000u8,
      0b1111_0000u8,
      0b1111_1000u8,
      0b1111_1100u8,
      0b1111_1110u8,
      0b1111_1111u8,
    ],
  );

  // Spread 11 can also be 8 + spread 3

  pub static SPREAD_11: Spread<6> = Spread::new(
    [
      0b1111_1111u8,
      0b0111_1111u8,
      0b0011_1111u8,
      0b0001_1111u8,
      0b0000_1111u8,
      0b0000_0111u8,
    ],
    [
      0b1110_0000u8,
      0b1111_0000u8,
      0b1111_1000u8,
      0b1111_1100u8,
      0b1111_1110u8,
      0b1111_1111u8,
    ],
  );

  // Spread 12 can also be 8 + spread 4
  pub static SPREAD_12: Spread<5> = Spread::new(
    [
      0b1111_1111u8,
      0b0111_1111u8,
      0b0011_1111u8,
      0b0001_1111u8,
      0b0000_1111u8,
    ],
    [
      0b1111_0000u8,
      0b1111_1000u8,
      0b1111_1100u8,
      0b1111_1110u8,
      0b1111_1111u8,
    ],
  );

  // Spread 13 can also be 8 + spread 5
  pub static SPREAD_13: Spread<4> = Spread::new(
    [0b1111_1111u8, 0b0111_1111u8, 0b0011_1111u8, 0b0001_1111u8],
    [0b1111_1000u8, 0b1111_1100u8, 0b1111_1110u8, 0b1111_1111u8],
  );

  // Spread 14 can also be 8 + spread 6
  pub static SPREAD_14: Spread<3> = Spread::new(
    [0b1111_1111u8, 0b0111_1111u8, 0b0011_1111u8],
    [0b1111_1100u8, 0b1111_1110u8, 0b1111_1111u8],
  );

  // Spread 15 can also be 8 + spread 7
  pub static SPREAD_15: Spread<2> = Spread::new(
    [0b1111_1111u8, 0b0111_1111u8],
    [0b1111_1110u8, 0b1111_1111u8],
  );

  pub static SPREAD_16: Spread<1> = Spread::new([0b1111_1111u8], [0b1111_1111u8]);
}

#[cfg(test)]
mod test {
  use crate::freelist::search::{NEEDLE_08, SPREAD_02};
  use crate::freelist::FreelistManager;
  use bbolt_engine::common::bitset::BitSet;
  use bbolt_engine::common::ids::{FreePageId, PageId};
  use itertools::Itertools;
  use parking_lot::Mutex;
  use std::collections::BTreeMap;
  use std::mem;
  /*  #[test]
  fn test_freelist_manager() {
    let mut freelist_manager = NaiveClosestFreelistManager::new(&[]);
    for i in 2..12 {
      freelist_manager.free(FreePageId::of(i));
    }
    for i in 20..30 {
      freelist_manager.free(FreePageId::of(i));
    }
    let l = freelist_manager.assign(PageId::from(330), 6);
    assert_eq!(20, freelist_manager.len());
  }*/

  #[derive(Debug, Copy, Clone)]
  pub enum FindResult {
    Needle(usize, u8),
    Pair((usize, u8), (usize, u8)),
    Range((usize, u8), (usize, u8)),
  }

  impl FindResult {
    pub fn min_dist_to(&self, lot: usize) -> usize {
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
        Some(r) => current_lot.abs_diff(self.partition) > r.min_dist_to(self.partition),
      }
    }

    pub fn submit_result(&mut self, result: FindResult) {
      match self.result.take() {
        None => self.result = Some(result),
        Some(found) => {
          if found.min_dist_to(self.partition) < result.min_dist_to(self.partition) {
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
      .filter(|(_, byte)| NEEDLE_08.any_mask()(byte))
    {
      println!("{:?} {:?}", a, b);
    }

    for (a, b) in v
      .iter()
      .enumerate()
      .rev()
      .filter(|(_, byte)| NEEDLE_08.any_mask()(byte))
    {
      println!("{:?} {:?}", a, b);
    }
  }

  #[test]
  fn test_find_spread_simple() {
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
    let store = Mutex::new(FindStore::new(3));
    for (a, b) in v
      .iter()
      .enumerate()
      .take_while(|(lot, _)| {
        if lot % 32 == 0 {
          (&store).lock().is_dist_exceeded(*lot)
        } else {
          true
        }
      })
      .tuple_windows()
      .filter(|((_, l), (_, r))| SPREAD_02.eq_mask()(l, r))
    {
      println!("{:?} {:?}", a, b);
    }

    for (a, b) in v
      .iter()
      .enumerate()
      .rev()
      .tuple_windows()
      .map(|(r, l)| (l, r))
      .filter(|((_, l), (_, r))| SPREAD_02.eq_mask()(l, r))
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

  pub enum LotType {
    Swap,
    Claimed(usize),
    Freed(usize),
    Array(Box<[u8]>),
    Vec(Vec<u8>),
  }

  impl LotType {
    #[inline]
    pub const fn claimed(page_size: usize) -> LotType {
      LotType::Claimed(page_size)
    }

    #[inline]
    pub const fn freed(page_size: usize) -> LotType {
      LotType::Freed(page_size)
    }

    pub fn array<T: Into<Box<[u8]>>>(a: T) -> LotType {
      LotType::Array(a.into())
    }

    pub fn vec<T: Into<Vec<u8>>>(a: T) -> LotType {
      LotType::Vec(a.into())
    }

    pub fn len(&self) -> usize {
      match self {
        LotType::Swap => unreachable!(),
        LotType::Claimed(page_size) => *page_size,
        LotType::Freed(page_size) => *page_size,
        LotType::Array(a) => a.len(),
        LotType::Vec(v) => v.len(),
      }
    }

    pub fn is_claimed(&self) -> bool {
      match self {
        LotType::Swap => unreachable!(),
        LotType::Claimed(_) => true,
        LotType::Freed(_) => false,
        LotType::Array(a) => a.iter().all(|x| *x == 0),
        LotType::Vec(v) => v.iter().all(|x| *x == 0),
      }
    }

    pub fn is_free(&self) -> bool {
      match self {
        LotType::Swap => unreachable!(),
        LotType::Claimed(_) => false,
        LotType::Freed(_) => true,
        LotType::Array(a) => a.iter().all(|x| *x != 0),
        LotType::Vec(v) => v.iter().all(|x| *x != 0),
      }
    }

    pub fn has_free(&self) -> bool {
      match self {
        LotType::Swap => unreachable!(),
        LotType::Claimed(_) => false,
        LotType::Freed(_) => true,
        LotType::Array(a) => a.iter().any(|x| *x != 0),
        LotType::Vec(v) => v.iter().any(|x| *x != 0),
      }
    }

    pub fn is_mut(&self) -> bool {
      match self {
        LotType::Vec(_) => true,
        _ => false,
      }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
      if !self.is_mut() {
        let mut swap = LotType::Swap;
        mem::swap(self, &mut swap);
        let v = match swap {
          LotType::Swap => unreachable!(),
          LotType::Claimed(page_size) => vec![0u8; page_size],
          LotType::Freed(page_size) => vec![u8::MAX; page_size],
          LotType::Array(a) => a.into(),
          LotType::Vec(_) => unreachable!(),
        };
        swap = LotType::Vec(v);
        mem::swap(self, &mut swap);
      }
      match self {
        LotType::Vec(v) => v,
        _ => unreachable!(),
      }
    }
  }

  pub struct FreePageStore {
    page_size: usize,
    store: BTreeMap<usize, LotType>,
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
        store.insert(i, LotType::Freed(page_size));
      }
      FreePageStore { page_size, store }
    }

    pub fn with_claimed_pages(page_size: usize, page_count: usize) -> FreePageStore {
      let mut store = BTreeMap::new();
      for i in 0..page_count {
        store.insert(i, LotType::Claimed(page_size));
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
        .or_insert(LotType::Freed(self.page_size))
        .get_mut()[lot_index]
        .set(offset);
    }

    pub fn claim<T: Into<PageId>>(&mut self, page_id: T) {
      let (store_index, lot_index, offset) = self.get_location(page_id);
      self
        .store
        .entry(store_index)
        .or_insert(LotType::Freed(self.page_size))
        .get_mut()[lot_index]
        .unset(offset);
    }

    pub fn find_near<T: Into<PageId>>(&self, page_id: T, len: usize) -> Option<FreePageId> {
      assert_ne!(len, 0);

      unimplemented!()
    }
  }

  #[test]
  pub fn near() {
    for i in 1..33u8 {
      let div = i / 8;
      let rem = i % 8;
      println!("{:?}: {:?} - {:?}", i, div, rem);
    }
  }
}
