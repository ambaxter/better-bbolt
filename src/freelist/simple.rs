use crate::freelist::masks::*;
use crate::freelist::search::SearchPattern;
use crate::freelist::{SearchResult, SearchStore};
use bbolt_engine::common::bitset::BitSet;
use bbolt_engine::common::ids::{EOFPageId, FreePageId, GetPageId, LotIndex, LotOffset, PageId};

pub struct SimpleFreePages {
  page_size: usize,
  eof: EOFPageId,
  store: Vec<u8>,
}

impl SimpleFreePages {
  pub fn with_free_pages(page_size: usize, eof: EOFPageId) -> SimpleFreePages {
    let last_page = eof.page_id() - 1;
    let (last_index, _) = last_page.lot_index_and_offset();
    let mut store = vec![u8::MAX; last_index.0];
    SimpleFreePages {
      page_size,
      eof,
      store,
    }
  }

  pub fn with_claimed_pages(page_size: usize, eof: EOFPageId) -> SimpleFreePages {
    let last_page = eof.page_id() - 1;
    let (last_index, _) = last_page.lot_index_and_offset();

    let mut store = vec![u8::MIN; last_index.0];
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

  #[cfg(test)]
  pub fn with_free_ids(page_size: usize, eof: EOFPageId, page_ids: &[u64]) -> SimpleFreePages {
    let mut store = SimpleFreePages::with_claimed_pages(page_size, eof);
    for id in page_ids.iter().map(|id| PageId::of(*id)) {
      store.free(id);
    }
    store
  }

  #[inline(always)]
  fn get_location<T: Into<PageId>>(&self, page_id: T) -> (LotIndex, LotOffset) {
    page_id.into().lot_index_and_offset()
  }

  pub fn is_free<T: Into<PageId>>(&self, page_id: T) -> bool {
    let (lot_index, offset) = self.get_location(page_id);
    assert!(lot_index.0 < self.store.len());
    self.store[lot_index.0].get(offset.0)
  }

  pub fn free<T: Into<PageId>>(&mut self, page_id: T) {
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

  pub fn find_near<T: Into<PageId>>(&self, goal_page_id: T, len: usize) -> Option<SearchResult> {
    let (goal_lot, _) = self.get_location(goal_page_id);
    let mut search_store = SearchStore::new(goal_lot);
    let s = SearchPattern::new(&self.store, goal_lot.0);
    assert_ne!(len, 0);
    // TODO: Surely there's a better way
    // Determine which searches are needed based off of the page length
    let results = match ((len / 16, len % 16), (len / 8, len % 8)) {
      // Special handling for the first 15 entries
      ((0, 1), _) => [
        s.needle_search(N1),
        s.needle_rsearch(N1),
        None,
        None,
        None,
        None,
      ],
      ((0, 2), _) => [
        s.needle_search(N2),
        s.needle_rsearch(N2),
        s.pair_search(BE2.into()),
        s.pair_rsearch(BE2.into()),
        None,
        None,
      ],
      ((0, 3), _) => [
        s.needle_search(N3),
        s.needle_rsearch(N3),
        s.pair_search(BE3.into()),
        s.pair_rsearch(BE3.into()),
        None,
        None,
      ],
      ((0, 4), _) => [
        s.needle_search(N4),
        s.needle_rsearch(N4),
        s.pair_search(BE4.into()),
        s.pair_rsearch(BE4.into()),
        None,
        None,
      ],
      ((0, 5), _) => [
        s.needle_search(N5),
        s.needle_rsearch(N5),
        s.pair_search(BE5.into()),
        s.pair_rsearch(BE5.into()),
        None,
        None,
      ],
      ((0, 6), _) => [
        s.needle_search(N6),
        s.needle_rsearch(N6),
        s.pair_search(BE6.into()),
        s.pair_rsearch(BE6.into()),
        None,
        None,
      ],
      ((0, 7), _) => [
        s.needle_search(N7),
        s.needle_rsearch(N7),
        s.pair_search(BE7.into()),
        s.pair_rsearch(BE7.into()),
        None,
        None,
      ],
      ((0, 8), _) => [
        s.needle_search(N8),
        s.needle_rsearch(N8),
        s.pair_search(BE8.into()),
        s.pair_rsearch(BE8.into()),
        None,
        None,
      ],
      ((0, 9), _) => [
        s.pair_search(BE9.into()),
        s.pair_rsearch(BE9.into()),
        s.boyer_moore_magiclen_search(1, EE1.into()),
        s.boyer_moore_magiclen_rsearch(1, EE1.into()),
        None,
        None,
      ],
      ((0, 10), _) => [
        s.pair_search(BE10.into()),
        s.pair_rsearch(BE10.into()),
        s.boyer_moore_magiclen_search(1, EE2.into()),
        s.boyer_moore_magiclen_rsearch(1, EE2.into()),
        s.boyer_moore_magiclen_search(1, BE2.into()),
        s.boyer_moore_magiclen_rsearch(1, BE2.into()),
      ],
      ((0, 11), _) => [
        s.pair_search(BE11.into()),
        s.pair_rsearch(BE11.into()),
        s.boyer_moore_magiclen_search(1, EE3.into()),
        s.boyer_moore_magiclen_rsearch(1, EE3.into()),
        s.boyer_moore_magiclen_search(1, BE3.into()),
        s.boyer_moore_magiclen_rsearch(1, BE3.into()),
      ],
      ((0, 12), _) => [
        s.pair_search(BE12.into()),
        s.pair_rsearch(BE12.into()),
        s.boyer_moore_magiclen_search(1, EE4.into()),
        s.boyer_moore_magiclen_rsearch(1, EE4.into()),
        s.boyer_moore_magiclen_search(1, BE4.into()),
        s.boyer_moore_magiclen_rsearch(1, BE4.into()),
      ],
      ((0, 13), _) => [
        s.pair_search(BE13.into()),
        s.pair_rsearch(BE13.into()),
        s.boyer_moore_magiclen_search(1, EE5.into()),
        s.boyer_moore_magiclen_rsearch(1, EE5.into()),
        s.boyer_moore_magiclen_search(1, BE5.into()),
        s.boyer_moore_magiclen_rsearch(1, BE5.into()),
      ],
      ((0, 14), _) => [
        s.pair_search(BE14.into()),
        s.pair_rsearch(BE14.into()),
        s.boyer_moore_magiclen_search(1, EE6.into()),
        s.boyer_moore_magiclen_rsearch(1, EE6.into()),
        s.boyer_moore_magiclen_search(1, BE6.into()),
        s.boyer_moore_magiclen_rsearch(1, BE6.into()),
      ],
      ((0, 15), _) => [
        s.boyer_moore_magiclen_search(1, EE7.into()),
        s.boyer_moore_magiclen_rsearch(1, EE7.into()),
        s.boyer_moore_magiclen_search(1, BE7.into()),
        s.boyer_moore_magiclen_rsearch(1, BE7.into()),
        None,
        None,
      ],
      (_, (m, 0)) => [
        s.boyer_moore_magiclen_search(m, EE8.into()),
        s.boyer_moore_magiclen_rsearch(m, EE8.into()),
        s.boyer_moore_magiclen_search(m, BE8.into()),
        s.boyer_moore_magiclen_rsearch(m, BE8.into()),
        None,
        None,
      ],
      (_, (m, 1)) => [
        s.boyer_moore_magiclen_search(m, EE1.into()),
        s.boyer_moore_magiclen_rsearch(m, EE1.into()),
        s.boyer_moore_magiclen_search(m - 1, BE9.into()),
        s.boyer_moore_magiclen_rsearch(m - 1, BE9.into()),
        None,
        None,
      ],
      (_, (m, 2)) => [
        s.boyer_moore_magiclen_search(m, EE2.into()),
        s.boyer_moore_magiclen_rsearch(m, EE2.into()),
        s.boyer_moore_magiclen_search(m, BE2.into()),
        s.boyer_moore_magiclen_rsearch(m, BE2.into()),
        s.boyer_moore_magiclen_search(m - 1, BE10.into()),
        s.boyer_moore_magiclen_rsearch(m - 1, BE10.into()),
      ],
      (_, (m, 3)) => [
        s.boyer_moore_magiclen_search(m, EE3.into()),
        s.boyer_moore_magiclen_rsearch(m, EE3.into()),
        s.boyer_moore_magiclen_search(m, BE3.into()),
        s.boyer_moore_magiclen_rsearch(m, BE3.into()),
        s.boyer_moore_magiclen_search(m - 1, BE11.into()),
        s.boyer_moore_magiclen_rsearch(m - 1, BE11.into()),
      ],
      (_, (m, 4)) => [
        s.boyer_moore_magiclen_search(m, EE4.into()),
        s.boyer_moore_magiclen_rsearch(m, EE4.into()),
        s.boyer_moore_magiclen_search(m, BE4.into()),
        s.boyer_moore_magiclen_rsearch(m, BE4.into()),
        s.boyer_moore_magiclen_search(m - 1, BE12.into()),
        s.boyer_moore_magiclen_rsearch(m - 1, BE12.into()),
      ],
      (_, (m, 5)) => [
        s.boyer_moore_magiclen_search(m, EE5.into()),
        s.boyer_moore_magiclen_rsearch(m, EE5.into()),
        s.boyer_moore_magiclen_search(m, BE5.into()),
        s.boyer_moore_magiclen_rsearch(m, BE5.into()),
        s.boyer_moore_magiclen_search(m - 1, BE13.into()),
        s.boyer_moore_magiclen_rsearch(m - 1, BE13.into()),
      ],
      (_, (m, 6)) => [
        s.boyer_moore_magiclen_search(m, EE6.into()),
        s.boyer_moore_magiclen_rsearch(m, EE6.into()),
        s.boyer_moore_magiclen_search(m, BE6.into()),
        s.boyer_moore_magiclen_rsearch(m, BE6.into()),
        s.boyer_moore_magiclen_search(m - 1, BE14.into()),
        s.boyer_moore_magiclen_rsearch(m - 1, BE14.into()),
      ],
      (_, (m, 7)) => [
        s.boyer_moore_magiclen_search(m, EE7.into()),
        s.boyer_moore_magiclen_rsearch(m, EE7.into()),
        s.boyer_moore_magiclen_search(m, BE7.into()),
        s.boyer_moore_magiclen_rsearch(m, BE7.into()),
        None,
        None,
      ],
      _ => unreachable!(),
    };
    for r in results {
      search_store.push(r);
    }
    search_store.get()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test() {
    let mut s = SimpleFreePages::with_free_ids(4096, EOFPageId::of(4097), &[12, 13, 14]);
    println!("{:?}", s.find_near(PageId::of(124), 64));
  }
}
