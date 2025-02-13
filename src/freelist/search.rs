use crate::freelist::masks::{NMask, PairMaskTest};
use crate::freelist::SearchResult;
use bbolt_engine::common::ids::{LotIndex, LotOffset};
use itertools::Itertools;
use std::iter::{repeat_n, RepeatN};
use std::ops::{Index, RangeBounds};
/*

The following code is derived from the boyer-moore-magiclen project
(https://github.com/magiclen/boyer-moore-magiclen) under the following license

MIT License

Copyright (c) 2019 magiclen.org (Ron Li)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

pub trait LenTrait {
  fn is_empty(&self) -> bool;

  fn len(&self) -> usize;
}

impl<T> LenTrait for T
where
  T: AsRef<[u8]>,
{
  #[inline]
  fn is_empty(&self) -> bool {
    self.as_ref().is_empty()
  }

  #[inline]
  fn len(&self) -> usize {
    self.as_ref().len()
  }
}

pub trait RangedIterator {
  fn iterate_from(
    &self, midpoint: usize,
  ) -> impl Iterator<Item = u8> + Sized + ExactSizeIterator + DoubleEndedIterator;
  fn iterate_to(
    &self, midpoint: usize,
  ) -> impl Iterator<Item = u8> + Sized + ExactSizeIterator + DoubleEndedIterator;
}

impl<T> RangedIterator for T
where
  T: AsRef<[u8]>,
{
  fn iterate_from(
    &self, midpoint: usize,
  ) -> impl Iterator<Item = u8> + Sized + ExactSizeIterator + DoubleEndedIterator {
    self.as_ref()[midpoint..].iter().copied()
  }

  fn iterate_to(
    &self, midpoint: usize,
  ) -> impl Iterator<Item = u8> + Sized + ExactSizeIterator + DoubleEndedIterator {
    self.as_ref()[..midpoint].iter().copied()
  }
}

#[derive(Clone, Copy)]
pub struct SearchPattern<'a, T>
where
  T: ?Sized,
{
  store: &'a T,
  goal_lot: usize,
}

impl<'a, T> SearchPattern<'a, T>
where
  T: RangedIterator + Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a T: IntoIterator<Item = &'a u8>,
  <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
{
  pub fn new(store: &'a T, goal_lot: usize) -> SearchPattern<'a, T> {
    SearchPattern { store, goal_lot }
  }

  fn bad_shift_index(free_bytes_len: usize, index: u8) -> usize {
    if index == u8::MAX {
      0
    } else {
      free_bytes_len
    }
  }

  fn repeat_iter(free_bytes_len: usize) -> RepeatN<u8> {
    repeat_n(u8::MAX, free_bytes_len)
  }

  fn index_pattern(free_bytes_len: usize, index: usize) -> u8 {
    assert!(index < free_bytes_len);
    u8::MAX
  }

  fn get_ends(&self, free_bytes_len: usize, shift_idx: usize) -> (Option<(usize, u8)>, Option<u8>) {
    let r_idx = shift_idx + free_bytes_len;
    let r = if r_idx <= self.store.len() - 1 {
      Some(self.store[r_idx])
    } else {
      None
    };
    let l = if shift_idx > 0 {
      let l_idx = shift_idx - 1;
      Some((l_idx, self.store[l_idx]))
    } else {
      None
    };
    (l, r)
  }

  pub fn needle_search<const N: usize>(&self, nmask: NMask<N>) -> Option<SearchResult> {
    self
      .store
      .iterate_from(self.goal_lot)
      .enumerate()
      .map(|(idx, byte)| (idx + self.goal_lot, byte))
      .filter_map(|(idx, byte)| nmask.match_byte_at(idx, byte))
      .map(|(idx, offset)| SearchResult::new(LotIndex(idx), offset))
      .next()
  }

  pub fn needle_rsearch<const N: usize>(&self, nmask: NMask<N>) -> Option<SearchResult> {
    self
      .store
      .iterate_to(self.goal_lot)
      .enumerate()
      .rev()
      .map(|(idx, byte)| (idx, byte))
      .filter_map(|(idx, byte)| nmask.match_byte_at(idx, byte))
      .map(|(idx, offset)| SearchResult::new(LotIndex(idx), offset))
      .next()
  }

  pub fn pair_search<const N: usize>(
    &self, pair_mask_test: PairMaskTest<N>,
  ) -> Option<SearchResult> {
    self
      .store
      .iterate_from(self.goal_lot)
      .enumerate()
      .map(|(idx, byte)| (idx + self.goal_lot, byte))
      .tuple_windows()
      .map(|(((l_idx, l_byte), (_, r_byte)))| (l_idx, l_byte, r_byte))
      .filter_map(|(l_idx, l_byte, r_byte)| pair_mask_test.match_bytes_at(l_idx, l_byte, r_byte))
      .map(|(idx, offset)| SearchResult::new(LotIndex(idx), offset))
      .next()
  }

  pub fn pair_rsearch<const N: usize>(
    &self, pair_mask_test: PairMaskTest<N>,
  ) -> Option<SearchResult> {
    self
      .store
      .iterate_to(self.goal_lot)
      .enumerate()
      .rev()
      .map(|(idx, byte)| (idx, byte))
      .tuple_windows()
      .map(|(((_, r_byte), (l_idx, l_byte)))| (l_idx, l_byte, r_byte))
      .filter_map(|(l_idx, l_byte, r_byte)| pair_mask_test.match_bytes_at(l_idx, l_byte, r_byte))
      .map(|(idx, offset)| SearchResult::new(LotIndex(idx), offset))
      .next()
  }

  pub fn boyer_moore_magiclen_search<const N: usize>(
    &self, free_bytes_len: usize, mask_test: PairMaskTest<N>,
  ) -> Option<SearchResult> {
    if self.store.len() == 0
      || free_bytes_len == 0
      || self.store.len() < free_bytes_len
      || self.store.len() < self.goal_lot
      || self.store.len() - self.goal_lot < free_bytes_len
    {
      return None;
    }

    let free_bytes_len_dec = free_bytes_len - 1;

    let last_pattern_char = u8::MAX;

    let mut shift = if free_bytes_len > self.goal_lot {
      0
    } else {
      self.goal_lot - free_bytes_len
    };

    let end_index = self.store.len() - free_bytes_len;

    'outer: loop {
      for (i, pc) in Self::repeat_iter(free_bytes_len).enumerate().rev() {
        if self.store[shift + i] != pc {
          let p = shift + free_bytes_len;
          if p == self.store.len() {
            break 'outer;
          }
          shift += Self::bad_shift_index(free_bytes_len, self.store[shift + free_bytes_len_dec])
            .max({
              let c = self.store[p];
              if c == last_pattern_char {
                1
              } else {
                Self::bad_shift_index(free_bytes_len, c) + 1
              }
            });
          if shift > end_index {
            break 'outer;
          }
          continue 'outer;
        }
      }
      let mut in_run = false;
      while shift > 0 && self.store[shift - 1] == u8::MAX {
        in_run = true;
        shift -= 1;
      }
      // test to see if the ends match
      let ends_match = {
        let mut end_match = mask_test.match_ends(self.get_ends(free_bytes_len, shift));
        if end_match.is_none() && in_run {
          end_match = mask_test.match_ends(self.get_ends(free_bytes_len, shift + 1));
        }
        end_match
      };
      if ends_match.is_some() {
        return ends_match.map(|(idx, offset)| SearchResult::new(LotIndex(idx), offset));
      }

      if shift == end_index {
        break;
      }

      shift += free_bytes_len;

      if shift > end_index {
        break;
      }
      continue;
    }

    None
  }

  pub fn boyer_moore_magiclen_rsearch<const N: usize>(
    &self, free_bytes_len: usize, mask_test: PairMaskTest<N>,
  ) -> Option<SearchResult> {
    if self.store.len() == 0
      || free_bytes_len == 0
      || self.store.len() < free_bytes_len
      || self.store.len() < self.goal_lot
      || self.store.len() - self.goal_lot < free_bytes_len
      //TODO: Is this right?
      || self.store.len() - 1 < self.goal_lot + free_bytes_len
    {
      return None;
    }

    let free_bytes_len_dec = free_bytes_len - 1;

    let first_pattern_char = u8::MAX;

    let mut shift = self.goal_lot + free_bytes_len;

    let start_index = free_bytes_len_dec;

    'outer: loop {
      for (i, pc) in Self::repeat_iter(free_bytes_len).enumerate() {
        if self.store[shift - free_bytes_len_dec + i] != pc {
          if shift < free_bytes_len {
            break 'outer;
          }
          let s = Self::bad_shift_index(free_bytes_len, self.store[shift - free_bytes_len_dec])
            .max({
              let c = self.store[shift - free_bytes_len];

              if c == first_pattern_char {
                1
              } else {
                Self::bad_shift_index(free_bytes_len, c) + 1
              }
            });
          if shift < s {
            break 'outer;
          }
          shift -= s;
          if shift < start_index {
            break 'outer;
          }
          continue 'outer;
        }
      }
      let mut in_run = false;
      while shift > 0 && self.store[shift - 1] == u8::MAX {
        in_run = true;
        shift -= 1;
      }
      // test to see if the ends match
      let ends_match = {
        let mut end_match = mask_test.match_ends(self.get_ends(free_bytes_len, shift));
        if end_match.is_none() && in_run {
          end_match = mask_test.match_ends(self.get_ends(free_bytes_len, shift + 1));
        }
        end_match
      };
      if ends_match.is_some() {
        return ends_match.map(|(idx, offset)| SearchResult::new(LotIndex(idx), offset));
      }

      if shift <= start_index {
        break;
      }

      shift -= free_bytes_len;
      if shift < start_index {
        break;
      }
    }

    None
  }
}

#[cfg(test)]
mod tests {
  use crate::freelist::masks::BE8;
  use crate::freelist::search::SearchPattern;

  #[test]
  pub fn test() {
    let v = vec![u8::MAX; 16];
    let midpoint = 8usize;
    let free_bytes_len = 3;
    let s = SearchPattern::new(&v, midpoint);
    let o = s.boyer_moore_magiclen_rsearch(free_bytes_len, BE8.into());
    println!("{:?}", o);
  }

  #[test]
  pub fn test_fwd() {
    let mut v = [
      0b1111_1110u8,
      255,
      255,
      255,
      0b0000_0001u8,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
    ];
    let free_bytes_len = 3;
    for midpoint in 0..16 {
      for i in 0..16 {
        let s = SearchPattern::new(&v, midpoint);
        let o = s.boyer_moore_magiclen_search(free_bytes_len, BE8.into());
        println!("m{}-r{}: {:?}", midpoint, i, o);
        v.rotate_right(1);
      }
    }
  }

  #[test]
  pub fn test_rev() {
    let mut v = [
      0b1111_1110u8,
      255,
      255,
      255,
      0b0000_0001u8,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
    ];
    let free_bytes_len = 3;
    for midpoint in 0..16 {
      for i in 0..16 {
        let s = SearchPattern::new(&v, midpoint);
        let o = s.boyer_moore_magiclen_rsearch(free_bytes_len, BE8.into());
        println!("m{}-r{}: {:?}", midpoint, i, o);
        v.rotate_right(1);
      }
    }
  }
}
