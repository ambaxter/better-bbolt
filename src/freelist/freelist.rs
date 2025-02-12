use std::cmp::Ordering;
use bbolt_engine::common::bitset::BitSet;
use bbolt_engine::common::ids::{FreePageId, LotIndex, LotOffset, PageId};

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

#[derive(Debug, Copy, Clone)]
pub struct FindResult {
  lot: LotIndex,
  offset: LotOffset,
}

impl FindResult {
  pub fn mid_dist_to(&self, goal_lot: LotIndex) -> usize {
    self.lot.abs_diff(goal_lot)
  }
}

pub struct FindStore {
  goal_lot: LotIndex,
  result: Option<FindResult>,
}

impl FindStore {
  pub fn new(goal_lot: LotIndex) -> FindStore {
    FindStore {
      goal_lot,
      result: None,
    }
  }

  pub fn push(&mut self, new: FindResult) {
    self.result = match self.result.take() {
      None => Some(new),
      Some(current) => {
        match current.mid_dist_to(self.goal_lot).cmp(&new.mid_dist_to(self.goal_lot)) {
          Ordering::Less => Some(current),
          Ordering::Equal => {
            if current.offset < new.offset {
              Some(current)
            } else {
              Some(new)
            }
          }
          Ordering::Greater => Some(new),
        }
      }
    };
  }
}

pub struct SimpleFreePageStore {
  page_size: usize,
  store: Vec<u8>,
}

impl SimpleFreePageStore {
  pub fn new(page_size: usize) -> SimpleFreePageStore {
    SimpleFreePageStore {
      page_size,
      store: Vec::new(),
    }
  }

  pub fn with_free_pages(page_size: usize, page_count: usize) -> SimpleFreePageStore {
    let mut store = vec![u8::MAX; page_count];
    SimpleFreePageStore { page_size, store }
  }

  pub fn with_claimed_pages(page_size: usize, page_count: usize) -> SimpleFreePageStore {
    let mut store = vec![u8::MIN; page_count];
    SimpleFreePageStore { page_size, store }
  }

  pub fn with_free_page_ids(
    page_size: usize, page_count: usize, page_ids: &[FreePageId],
  ) -> SimpleFreePageStore {
    let mut store = SimpleFreePageStore::with_claimed_pages(page_size, page_count);
    for id in page_ids {
      store.free(*id);
    }
    store
  }

  fn get_location<T: Into<PageId>>(&self, page_id: T) -> (LotIndex, LotOffset) {
    let id = page_id.into().0;
    let store_lot = LotIndex((id / 8) as usize);
    let offset = LotOffset((id % 8) as u8);
    (store_lot, offset)
  }

  // TODO: Handle len/overflow
  pub fn free<T: Into<FreePageId>>(&mut self, page_id: T) {
    let (store_lot, offset) = self.get_location(page_id.into());
    assert!(store_lot.0 < self.store.len());
    self.store[store_lot.0].set(offset.0);
  }

  pub fn claim<T: Into<PageId>>(&mut self, page_id: T) {
    let (store_lot, offset) = self.get_location(page_id);
    assert!(store_lot.0 < self.store.len());
    self.store[store_lot.0].unset(offset.0);
  }

  pub fn len(&self) -> usize {
    self.store.len() * self.page_size * 8
  }

  pub fn find_near<T: Into<PageId>>(&self, goal_page_id: T, len: usize) -> Option<FreePageId> {
    let page_id = goal_page_id.into();
    let (lot_index, _) = self.get_location(page_id);
    let mut result = FindStore::new(lot_index);
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
}
