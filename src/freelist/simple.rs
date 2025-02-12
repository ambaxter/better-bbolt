use bbolt_engine::common::bitset::BitSet;
use bbolt_engine::common::ids::{EOFPageId, FreePageId, LotIndex, LotOffset, PageId};
use crate::freelist::SearchStore;

pub struct SimpleFreePageStore {
  page_size: usize,
  eof: EOFPageId,
  store: Vec<u8>,
}

impl SimpleFreePageStore {
  pub fn new(page_size: usize, eof: EOFPageId) -> SimpleFreePageStore {
    SimpleFreePageStore {
      page_size,
      eof,
      store: Vec::new(),
    }
  }

  pub fn with_free_pages(page_size: usize, eof: EOFPageId , lot_count: usize) -> SimpleFreePageStore {
    let mut store = vec![u8::MAX; lot_count];
    SimpleFreePageStore { page_size, eof,  store }
  }

  pub fn with_claimed_pages(page_size: usize, eof: EOFPageId, lot_count: usize) -> SimpleFreePageStore {
    let mut store = vec![u8::MIN; lot_count];
    SimpleFreePageStore { page_size, eof, store }
  }

  pub fn with_free_page_ids(
    page_size: usize, eof: EOFPageId, lot_count: usize, page_ids: &[FreePageId],
  ) -> SimpleFreePageStore {
    let mut store = SimpleFreePageStore::with_claimed_pages(page_size, eof, lot_count);
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
    let mut result = SearchStore::new(lot_index);
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
