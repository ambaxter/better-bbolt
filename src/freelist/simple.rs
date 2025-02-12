use crate::freelist::SearchStore;
use bbolt_engine::common::bitset::BitSet;
use bbolt_engine::common::ids::{EOFPageId, FreePageId, GetPageId, LotIndex, LotOffset, PageId};

pub struct SimpleFreePages {
  page_size: usize,
  eof: EOFPageId,
  store: Vec<u8>,
}

impl SimpleFreePages {
  pub fn new(page_size: usize, eof: EOFPageId) -> SimpleFreePages {
    SimpleFreePages {
      page_size,
      eof,
      store: Vec::new(),
    }
  }

  pub fn with_free_pages(page_size: usize, eof: EOFPageId) -> SimpleFreePages {
    let lot_count = (eof.page_id().0 - 1) as usize;
    let mut store = vec![u8::MAX; lot_count];
    SimpleFreePages {
      page_size,
      eof,
      store,
    }
  }

  pub fn with_claimed_pages(page_size: usize, eof: EOFPageId) -> SimpleFreePages {
    let lot_count = (eof.page_id().0 - 1) as usize;
    let mut store = vec![u8::MIN; lot_count];
    SimpleFreePages {
      page_size,
      eof,
      store,
    }
  }

  pub fn with_free_page_ids(
    page_size: usize, eof: EOFPageId, page_ids: &[FreePageId],
  ) -> SimpleFreePages {
    let mut store = SimpleFreePages::with_claimed_pages(page_size, eof);
    for id in page_ids {
      store.free(*id);
    }
    store
  }

  fn get_location<T: Into<PageId>>(&self, page_id: T) -> (LotIndex, LotOffset) {
    page_id.into().lot_index_and_offset()
  }

  pub fn is_free<T: Into<PageId>>(&self, page_id: T) -> bool {
    let (lot_index, offset) = self.get_location(page_id);
    assert!(lot_index.0 < self.store.len());
    self.store[lot_index.0].get(offset.0)
  }

  pub fn free<T: Into<FreePageId>>(&mut self, page_id: T) {
    let (lot_index, offset) = self.get_location(page_id);
    assert!(lot_index.0 < self.store.len());
    self.store[lot_index.0].set(offset.0);
  }

  pub fn claim<T: Into<PageId>>(&mut self, page_id: T) {
    let (store_lot, offset) = self.get_location(page_id);
    assert!(store_lot.0 < self.store.len());
    self.store[store_lot.0].unset(offset.0);
  }

  pub fn len(&self) -> usize {
    self.store.len() * 8
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
