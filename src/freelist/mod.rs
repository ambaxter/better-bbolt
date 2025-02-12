use bbolt_engine::common::ids::{LotIndex, LotOffset};
use std::cmp::Ordering;

pub mod freelist;
pub mod search;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct SearchResult {
  pub idx: LotIndex,
  pub offset: LotOffset,
}

impl SearchResult {
  pub fn new(idx: LotIndex, offset: LotOffset) -> Self {
    SearchResult { idx, offset }
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

  fn push(&mut self, result: SearchResult) {
    if self.best.is_none() {
      self.best = Some(result);
    } else {
      let best = self.best.take().unwrap();
      let best_dist = best.idx.abs_diff(self.goal_idx);
      let result_dist = result.idx.abs_diff(self.goal_idx);
      match best_dist.cmp(&result_dist) {
        Ordering::Less => {
          self.best = Some(best);
        }
        Ordering::Equal => {
          if best.offset < result.offset {
            self.best = Some(best);
          } else {
            self.best = Some(result);
          }
        }
        Ordering::Greater => {
          self.best = Some(result);
        }
      }
    }
  }
}
