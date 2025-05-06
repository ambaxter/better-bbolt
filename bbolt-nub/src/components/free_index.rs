use crate::common::id::{DbId, DiskPageId, DiskPageTranslator, EOFPageId, FreelistPageId, NodePageId};
use crate::io::transmogrify::direct::DirectTransmogrify;
use rangemap::RangeSet;
use std::cmp::min_by;
use std::collections::BTreeSet;
use std::iter::FusedIterator;
use std::ops::{AddAssign, Range};

pub struct FreeIndex<T> {
  page_translator: T,
  ranges: RangeSet<DiskPageId>,
  singles: BTreeSet<DiskPageId>,
  original_eof: EOFPageId,
  current_eof: EOFPageId,
}

#[derive(Debug, Clone)]
enum FreelistRange {
  Single(DiskPageId),
  Range(Range<DiskPageId>),
}

struct FreelistRangeIter<I> {
  fuse: bool,
  current_range: Range<DiskPageId>,
  inner: I
}

impl<I> FreelistRangeIter<I> where I: Iterator<Item = DiskPageId> {
  fn new(mut inner: I) -> Self {
    let (current_range, fuse) = if let Some(page) = inner.next() {
      (page..page + 1, false)
    } else {
      (DiskPageId::of(0)..DiskPageId::of(1), true)
    };
    FreelistRangeIter {
      fuse,
      current_range,
      inner,
    }
  }
}

impl<I> Iterator for FreelistRangeIter<I> where I: Iterator<Item = DiskPageId> {
  type Item = FreelistRange;

  fn next(&mut self) -> Option<Self::Item> {
    if self.fuse {
      return None;
    }
    loop {
      if let Some(page) = self.inner.next() {
        if self.current_range.end == page {
          self.current_range.end = page + 1;
        } else {
          return if self.current_range.end.0 - self.current_range.start.0 == 1 {
            let single = self.current_range.start;
            self.current_range = page..page + 1;
            Some(FreelistRange::Single(single))
          } else {
            let range = self.current_range.clone();
            self.current_range = page..page + 1;
            Some(FreelistRange::Range(range))
          }
        }
      }else {
        self.fuse = true;
        return Some(FreelistRange::Range(self.current_range.clone()));
      }
    }
  }
}

impl<I> FusedIterator for FreelistRangeIter<I> where I: Iterator<Item = DiskPageId> {}

impl<D> FreeIndex<D>
where
  D: DiskPageTranslator,
{
  pub fn new<T: IntoIterator<Item = DiskPageId>>(
    page_translator: D, free_ids: T, eof_page_id: EOFPageId,
  ) -> Self {
    let mut ranges = RangeSet::new();
    let mut singles = BTreeSet::new();
    for r in FreelistRangeIter::new(free_ids.into_iter()) {
      match r {
        FreelistRange::Single(s) => {singles.insert(s);},
        FreelistRange::Range(r) => ranges.insert(r),
      }
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

    let single_search = || {
      if len == 1 {
        let (left_entry, right_entry) = rayon::join(
          || {
            self
              .singles
              .range(DiskPageId(0)..desired)
              .rev()
              .next()
              .copied()
          },
          || {
            self
              .singles
              .range(desired..self.current_eof.0)
              .next()
              .copied()
          },
        );
        min_option(left_entry, right_entry)
      } else {
        None
      }
    };

    let range_search = || {
      let (left_entry, right_entry) = rayon::join(
        || {
          self
            .ranges
            .overlapping(DiskPageId(0)..desired)
            .rev()
            .filter(|range| range.end.0 - range.start.0 >= len)
            .map(|range| range.start)
            .next()
        },
        || {
          self
            .ranges
            .overlapping(desired..self.current_eof.0)
            .filter(|range| range.end.0 - range.start.0 >= len)
            .map(|range| range.start)
            .next()
        },
      );
      min_option(left_entry, right_entry)
    };

    let (single_entry, range_entry) = rayon::join(single_search, range_search);

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
