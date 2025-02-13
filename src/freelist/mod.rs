use bbolt_engine::common::ids::{FreePageId, LotIndex, LotOffset, PageId};
use std::cmp::Ordering;

pub mod masks;
pub mod search;
pub mod simple;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct SearchResult {
  pub idx: LotIndex,
  pub offset: LotOffset,
}

impl SearchResult {
  pub fn new(idx: LotIndex, offset: LotOffset) -> Self {
    SearchResult { idx, offset }
  }

  pub fn mid_dist_to(&self, goal_lot: LotIndex) -> usize {
    self.idx.abs_diff(goal_lot)
  }
}

#[derive(Debug, Clone)]
pub struct SearchStore {
  goal_lot: LotIndex,
  best: Option<SearchResult>,
}

impl SearchStore {
  fn new(goal_lot: LotIndex) -> Self {
    SearchStore {
      goal_lot,
      best: None,
    }
  }

  fn push(&mut self, new: Option<SearchResult>) {
    self.best = match (self.best.take(), new) {
      (None, None) => None,
      (Some(best), None) => Some(best),
      (None, Some(new)) => Some(new),
      (Some(best), Some(new)) => {
        match best
          .mid_dist_to(self.goal_lot)
          .cmp(&new.mid_dist_to(self.goal_lot))
        {
          Ordering::Less => Some(best),
          Ordering::Equal => {
            if best.offset < new.offset {
              Some(best)
            } else {
              Some(new)
            }
          }
          Ordering::Greater => Some(new),
        }
      }
    };
  }

  fn get(self) -> Option<SearchResult> {
    self.best
  }
}

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
