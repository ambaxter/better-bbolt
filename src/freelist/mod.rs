use bbolt_engine::common::ids::{AssignedPageId, FreePageId, LotIndex, LotOffset, PageId};
use std::cmp::Ordering;

pub mod masks;
pub mod search;
pub mod simple;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MaskDirective {
  Left(u8),
  Right(u8),
  Pair(u8, u8),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MatchLocation {
  pub index: LotIndex,
  pub offset: LotOffset,
}

impl MatchLocation {
  #[inline]
  pub fn new(index: usize, offset: LotOffset) -> Self {
    MatchLocation {
      index: LotIndex(index),
      offset,
    }
  }

  #[inline]
  pub fn mid_dist_to(&self, goal_lot: LotIndex) -> usize {
    self.index.abs_diff(goal_lot)
  }
}

impl From<MatchLocation> for AssignedPageId {
  fn from(value: MatchLocation) -> Self {
    AssignedPageId::new(PageId::of(
      (value.index.0 as u64 * 8) + (value.offset.0 as u64),
    ))
  }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MatchResult {
  match_location: MatchLocation,
  mask_directive: MaskDirective,
  run_length: usize,
}

impl MatchResult {
  #[inline]
  pub fn left(index: usize, offset: LotOffset, l_mask: u8) -> MatchResult {
    MatchResult {
      match_location: MatchLocation::new(index, offset),
      mask_directive: MaskDirective::Left(l_mask),
      run_length: 0,
    }
  }

  #[inline]
  pub fn right(index: usize, offset: LotOffset, r_mask: u8) -> MatchResult {
    MatchResult {
      match_location: MatchLocation::new(index, offset),
      mask_directive: MaskDirective::Right(r_mask),
      run_length: 0,
    }
  }

  #[inline]
  pub fn pair(index: usize, offset: LotOffset, l_mask: u8, r_mask: u8) -> MatchResult {
    MatchResult {
      match_location: MatchLocation::new(index, offset),
      mask_directive: MaskDirective::Pair(l_mask, r_mask),
      run_length: 0,
    }
  }

  #[inline]
  pub fn with_length(mut self, run_length: usize) -> Self {
    self.run_length = run_length;
    self
  }
}

#[derive(Debug, Clone)]
pub struct SearchStore {
  goal_lot: LotIndex,
  best: Option<MatchResult>,
}

impl SearchStore {
  #[inline]
  fn new(goal_lot: LotIndex) -> Self {
    SearchStore {
      goal_lot,
      best: None,
    }
  }

  fn push(&mut self, new: Option<MatchResult>) {
    self.best = match (self.best.take(), new) {
      (None, None) => None,
      (Some(best), None) => Some(best),
      (None, Some(new)) => Some(new),
      (Some(best), Some(new)) => {
        match best
          .match_location
          .mid_dist_to(self.goal_lot)
          .cmp(&new.match_location.mid_dist_to(self.goal_lot))
        {
          Ordering::Less => Some(best),
          Ordering::Equal => {
            if best.match_location.offset < new.match_location.offset {
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

  fn take(self) -> Option<MatchResult> {
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
