use crate::common::id::{DiskPageId, DiskPageTranslator, EOFPageId, FreelistPageId, NodePageId};
use crate::io::transmogrify::direct::DirectTransmogrify;
use rangemap::RangeSet;
use std::cmp::min_by;
use std::collections::BTreeSet;
use std::ops::AddAssign;

pub struct FreeIndex<T> {
  page_translator: T,
  ranges: RangeSet<DiskPageId>,
  singles: BTreeSet<DiskPageId>,
  original_eof: EOFPageId,
  current_eof: EOFPageId,
}

impl<D> FreeIndex<D>
where
  D: DiskPageTranslator,
{
  pub fn new<T: IntoIterator<Item = DiskPageId>>(
    page_translator: D, free_ids: T, eof_page_id: EOFPageId,
  ) -> Self {
    let iter = free_ids.into_iter().map(|d| d..d + 1);
    let mut ranges = RangeSet::from_iter(iter);
    let mut singles = BTreeSet::new();
    // Someone somewhere is going to laugh at this
    for r in ranges.iter().filter(|r| r.end.0 - r.start.0 == 1) {
      singles.insert(r.start);
    }
    for entry in &singles {
      ranges.remove(*entry..*entry + 1);
    }
    Self {
      page_translator,
      ranges,
      singles,
      original_eof: eof_page_id,
      current_eof: eof_page_id,
    }
  }

  #[inline]
  pub fn original_eof(&self) -> EOFPageId {
    self.original_eof
  }

  #[inline]
  pub fn current_eof(&self) -> EOFPageId {
    self.current_eof
  }

  pub fn required_file_growth(&self) -> u64 {
    self.current_eof.0.0 - self.original_eof.0.0
  }

  pub fn assign_node(&mut self, desired: NodePageId, len: u64) -> NodePageId {
    let desired_disk = self.page_translator.node_to_disk(desired);
    let assigned_disk = self.assign_disk(desired_disk, len);
    self.page_translator.disk_to_node(assigned_disk)
  }

  pub fn assign_freelist(&mut self, desired: FreelistPageId, len: u64) -> FreelistPageId {
    let desired_disk = self.page_translator.freelist_to_disk(desired);
    let assigned_disk = self.assign_disk(desired_disk, len);
    self.page_translator.disk_to_freelist(assigned_disk)
  }

  fn assign_disk(&mut self, desired: DiskPageId, len: u64) -> DiskPageId {
    let min_disk = |x: &DiskPageId, y: &DiskPageId| (&(x.0 - desired.0)).cmp(&(y.0 - desired.0));
    let min_option = |left_entry: Option<DiskPageId>, right_entry: Option<DiskPageId>| match (
      left_entry,
      right_entry,
    ) {
      (Some(left_entry), Some(right_entry)) => Some(min_by(left_entry, right_entry, min_disk)),
      (None, Some(right_entry)) => Some(right_entry),
      (Some(left_entry), None) => Some(left_entry),
      (None, None) => None,
    };
    let left_range = DiskPageId(0)..desired;
    let right_range = desired..self.current_eof.0;
    let single_entry = if len == 1 {
      let left_entry = self.singles.range(left_range.clone()).rev().next().copied();
      let right_entry = self.singles.range(right_range.clone()).next().copied();
      min_option(left_entry, right_entry)
    } else {
      None
    };

    let range_entry = {
      let left_entry = self
        .ranges
        .overlapping(left_range)
        .rev()
        .filter(|range| range.end.0 - range.start.0 >= len)
        .map(|range| range.start)
        .next();
      let right_entry = self
        .ranges
        .overlapping(right_range)
        .filter(|range| range.end.0 - range.start.0 >= len)
        .map(|range| range.start)
        .next();
      min_option(left_entry, right_entry)
    };

    match (single_entry, range_entry) {
      (Some(single_entry), Some(range_entry)) => {
        if single_entry.0 - desired.0 <= range_entry.0 - desired.0 {
          self.singles.remove(&single_entry);
          single_entry
        } else {
          self.ranges.remove(range_entry..range_entry + len);
          range_entry
        }
      }
      (Some(single_entry), None) => {
        self.singles.remove(&single_entry);
        single_entry
      }
      (None, Some(range_entry)) => {
        self.ranges.remove(range_entry..range_entry + len);
        range_entry
      }
      (None, None) => {
        let new_disk_page = self.current_eof.0;
        self.current_eof.0 += len;
        new_disk_page
      }
    }
  }
}
