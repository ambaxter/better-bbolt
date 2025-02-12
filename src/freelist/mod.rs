use bbolt_engine::common::ids::{FreePageId, LotIndex, LotOffset, PageId};
use std::cmp::Ordering;

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
  pub goal_idx: LotIndex,
  pub best: Option<SearchResult>,
}

impl SearchStore {
  fn new(goal_idx: LotIndex) -> Self {
    SearchStore {
      goal_idx,
      best: None,
    }
  }

  fn push(&mut self, new: SearchResult) {
    self.best = match self.best.take() {
      None => Some(new),
      Some(best) => {
        match best
          .mid_dist_to(self.goal_idx)
          .cmp(&new.mid_dist_to(self.goal_idx))
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
