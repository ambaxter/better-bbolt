use bbolt_index::common::ids::{FreePageId, GetPageId, PageId};
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
/*
//TODO: This will be so much easier once StepBy gets stabilized
pub struct NaiveClosestFreelistManager {
  freelist: RangeSet<u64>,
  len: usize,
}

impl FreelistManager for NaiveClosestFreelistManager {
  fn new(freelist_array: &[FreePageId]) -> Self {
    let len = freelist_array.len();
    let mut freelist = RangeSet::new();
    for free_page_id in freelist_array {
      let page_id = free_page_id.page_id().0;
      freelist.insert(page_id..page_id + 1)
    }

    NaiveClosestFreelistManager { freelist, len }
  }

  fn free(&mut self, free_page_id: FreePageId) {
    let page_id = free_page_id.page_id().0;
    self.freelist.insert(page_id..page_id + 1);
    self.len += 1;
  }

  fn assign(&mut self, parent: PageId, len: usize) -> Option<FreePageId> {
    let len = len as u64;
    let parent_page = parent.0;
    let l_overlap = self.freelist.overlapping(0..parent_page);
    let l_match = l_overlap
      .filter(|r| r.end - r.start >= len)
      .next_back()
      .cloned();
    // Note: this will not see the veeerrrrryyyyy last page due to rangemap limitations. If we need to we have other problems
    let r_overlap = self.freelist.overlapping(parent_page..u64::MAX);
    let r_match = r_overlap.filter(|r| r.end - r.start >= len).next().cloned();
    let m = match (l_match, r_match) {
      (Some(left_match), Some(right_match)) => {
        let l_dist = parent_page.abs_diff(left_match.end);
        let r_dist = parent_page.abs_diff(right_match.start);
        // Closer to the left match
        if l_dist < r_dist {
          Some(left_match.end - len..left_match.end)
        } else {
          Some(right_match.start..right_match.start + len)
        }
      }
      (Some(left_match), None) => Some(left_match.end - len..left_match.end),
      (None, Some(right_match)) => Some(right_match.start..right_match.start + len),
      (None, None) => None,
    }?;
    let free_page_id = FreePageId::of(m.start);
    self.freelist.remove(m);
    self.len -= len as usize;
    Some(free_page_id)
  }

  fn len(&self) -> usize {
    self.len
  }

  fn write(&self, freelist: &mut [FreePageId]) {
    assert!(self.len() <= freelist.len());
    let freelist_iter = self.freelist.iter().flat_map(|range| range.clone());

    for (i, free_page) in izip!(freelist_iter, &mut *freelist) {
      *free_page = FreePageId::of(i);
    }
    freelist.sort()
  }
}*/

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
  use std::collections::BTreeMap;
  use crate::freelist::search::{NEEDLE_08, SPREAD_02};
  use crate::freelist::{FreelistManager};
  use bbolt_index::common::ids::{FreePageId, LotId, LotKey, PageId};
  use itertools::Itertools;
  use parking_lot::Mutex;
  use bbolt_index::common::bitset::BitSet;
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


  // TODO: Redo spread bitmaps? I don't think I did them right
  #[test]
  fn test_tree() {
    let mut i = 0b0000_0001u8;
    i = 1 << 0;
    i <<= 7;
    let lot_key = LotKey::from_page_id_and_size(PageId::of(8192), 4096);
    let (lot_id, offset) = LotId::from_page_id(PageId::of(8193));
    let mut tree = BTreeMap::new();
    tree.insert(LotId::of(0), vec![0u8; 4096]);
    tree.insert(LotId::of(4096), vec![0u8; 4096]);
    tree.insert(LotId::of(4096 * 2), vec![0u8; 4096]);

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

}
