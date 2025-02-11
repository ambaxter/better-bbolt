use crate::freelist::search::masks::{GetLotOffset, NMask, PairMaskTest};
use bbolt_engine::common::ids::LotOffset;
use itertools::Itertools;
use std::iter::{repeat_n, RepeatN};
use std::ops::{Index, RangeBounds};

pub mod masks {
  use bbolt_engine::common::ids::LotOffset;
  use itertools::izip;

  pub trait GetLotOffset {
    fn high_offset(self) -> LotOffset;
  }

  impl GetLotOffset for u8 {
    #[inline]
    fn high_offset(self) -> LotOffset {
      let l_count = self.count_ones();
      LotOffset(8 - l_count as u8)
    }
  }

  // single needle mask
  #[derive(Debug, Copy, Clone)]
  pub struct NMask<const N: usize>(pub [u8; N]);

  impl<const N: usize> NMask<N> {
    pub fn match_byte_at(&self, idx: usize, byte: u8) -> Option<(usize, LotOffset)> {
      self
        .0
        .iter()
        .enumerate()
        .filter_map(|(mask_idx, &mask)| {
          if (byte & mask) == mask {
            Some((idx, LotOffset(mask_idx as u8)))
          } else {
            None
          }
        })
        .next()
    }
  }

  pub const N1: NMask<8> = NMask([
    0b0000_0001u8,
    0b0000_0010u8,
    0b0000_0100u8,
    0b0000_1000u8,
    0b0001_0000u8,
    0b0010_0000u8,
    0b0100_0000u8,
    0b1000_0000u8,
  ]);

  pub const N2: NMask<7> = NMask([
    0b0000_0011u8,
    0b0000_0110u8,
    0b0000_1100u8,
    0b0001_1000u8,
    0b0011_0000u8,
    0b0110_0000u8,
    0b1100_0000u8,
  ]);

  pub const N3: NMask<6> = NMask([
    0b0000_0111u8,
    0b0000_1110u8,
    0b0001_1100u8,
    0b0011_1000u8,
    0b0111_0000u8,
    0b1110_0000u8,
  ]);

  pub const N4: NMask<5> = NMask([
    0b0000_1111u8,
    0b0001_1110u8,
    0b0011_1100u8,
    0b0111_1000u8,
    0b1111_0000u8,
  ]);

  pub const N5: NMask<4> = NMask([0b0001_1111u8, 0b0011_1110u8, 0b0111_1100u8, 0b1111_1000u8]);

  pub const N6: NMask<3> = NMask([0b0011_1111u8, 0b0111_1110u8, 0b1111_1100u8]);

  // I’ve killed worse than you on my way to real problems - Commander Shepard
  pub const N7: NMask<2> = NMask([0b0111_1111u8, 0b1111_1110u8]);

  pub const N8: NMask<1> = NMask([0b1111_1111u8]);

  // either end mask
  #[derive(Debug, Copy, Clone)]
  pub struct EEMask(u8, u8);

  impl EEMask {
    pub fn match_bytes_at(
      &self, l_idx: usize, l_byte: u8, r_byte: u8,
    ) -> Option<(usize, LotOffset)> {
      if self.0 & l_byte == self.0 {
        Some((l_idx, self.0.high_offset()))
      } else if self.1 & r_byte == self.1 {
        Some((l_idx + 1, LotOffset(0)))
      } else {
        None
      }
    }
    pub fn match_ends(
      &self, l_end: Option<(usize, u8)>, r_end: Option<u8>,
    ) -> Option<(usize, LotOffset)> {
      match (l_end, r_end) {
        (Some((l_idx, l_byte)), _) if self.0 & l_byte == self.0 => {
          Some((l_idx, self.0.high_offset()))
        }
        (Some((l_idx, _)), Some(r_byte)) if self.1 & r_byte == self.1 => {
          Some((l_idx + 1, LotOffset(0)))
        }
        // Note: (None, Some) will never happen. If the left side is None that means we've reached
        // the beginning of the file and the first 2 pages of the database are always meta pages.
        _ => None,
      }
    }
  }

  pub const EE1: EEMask = EEMask(0b1000_0000u8, 0b0000_0001u8);
  pub const EE2: EEMask = EEMask(0b1100_0000u8, 0b0000_0011u8);
  pub const EE3: EEMask = EEMask(0b1110_0000u8, 0b0000_0111u8);
  pub const EE4: EEMask = EEMask(0b1111_0000u8, 0b0000_1111u8);
  pub const EE5: EEMask = EEMask(0b1111_1000u8, 0b0001_1111u8);
  pub const EE6: EEMask = EEMask(0b1111_1100u8, 0b0011_1111u8);
  pub const EE7: EEMask = EEMask(0b1111_1110u8, 0b0111_1111u8);

  #[derive(Debug, Copy, Clone)]
  pub struct BEMask<const N: usize>(pub [u8; N], [u8; N]);

  impl<const N: usize> BEMask<N> {
    pub fn match_bytes_at(
      &self, l_idx: usize, l_byte: u8, r_byte: u8,
    ) -> Option<(usize, LotOffset)> {
      izip!(self.0, self.1)
        .filter_map(|(l_mask, r_mask)| {
          if (l_byte & l_mask) == l_mask && (r_byte & r_mask) == r_mask {
            Some(l_mask.high_offset())
          } else {
            None
          }
        })
        .next()
        .map(|offset| (l_idx, offset))
    }

    pub fn match_ends(
      &self, l_end: Option<(usize, u8)>, r_end: Option<u8>,
    ) -> Option<(usize, LotOffset)> {
      match (l_end, r_end) {
        (Some((l_idx, l_byte)), Some(r_byte)) => self.match_bytes_at(l_idx, l_byte, r_byte),
        _ => None,
      }
    }
  }

  pub const BE2: BEMask<1> = BEMask([0b1000_0000u8], [0b0000_0001u8]);

  pub const BE3: BEMask<2> = BEMask(
    [0b1100_0000u8, 0b1000_0000u8],
    [0b0000_0001u8, 0b0000_0011u8],
  );

  pub const BE4: BEMask<3> = BEMask(
    [0b1110_0000u8, 0b1100_0000u8, 0b1000_0000u8],
    [0b0000_0001u8, 0b0000_0011u8, 0b0000_0111u8],
  );

  pub const BE5: BEMask<4> = BEMask(
    [0b1111_0000u8, 0b1110_0000u8, 0b1100_0000u8, 0b1000_0000u8],
    [0b0000_0001u8, 0b0000_0011u8, 0b0000_0111u8, 0b0000_1111u8],
  );

  pub const BE6: BEMask<5> = BEMask(
    [
      0b1111_1000u8,
      0b1111_0000u8,
      0b1110_0000u8,
      0b1100_0000u8,
      0b1000_0000u8,
    ],
    [
      0b0000_0001u8,
      0b0000_0011u8,
      0b0000_0111u8,
      0b0000_1111u8,
      0b0001_1111u8,
    ],
  );

  pub const BE7: BEMask<6> = BEMask(
    [
      0b1111_1100u8,
      0b1111_1000u8,
      0b1111_0000u8,
      0b1110_0000u8,
      0b1100_0000u8,
      0b1000_0000u8,
    ],
    [
      0b0000_0001u8,
      0b0000_0011u8,
      0b0000_0111u8,
      0b0000_1111u8,
      0b0001_1111u8,
      0b0011_1111u8,
    ],
  );

  pub const BE8: BEMask<7> = BEMask(
    [
      0b1111_1110u8,
      0b1111_1100u8,
      0b1111_1000u8,
      0b1111_0000u8,
      0b1110_0000u8,
      0b1100_0000u8,
      0b1000_0000u8,
    ],
    [
      0b0000_0001u8,
      0b0000_0011u8,
      0b0000_0111u8,
      0b0000_1111u8,
      0b0001_1111u8,
      0b0011_1111u8,
      0b0111_1111u8,
    ],
  );

  // TODO: Would this be better as a trait?
  // Something to test later
  #[derive(Clone, Copy)]
  pub enum PairMaskTest<const N: usize> {
    Either(EEMask),
    Both(BEMask<N>),
  }

  impl PairMaskTest<0> {
    pub fn new_either(either: EEMask) -> PairMaskTest<0> {
      Self::Either(either)
    }
  }
  impl<const N: usize> PairMaskTest<N> {
    pub fn new_both(both: BEMask<N>) -> PairMaskTest<N> {
      Self::Both(both)
    }

    pub fn match_bytes_at(
      &self, l_idx: usize, l_byte: u8, r_byte: u8,
    ) -> Option<(usize, LotOffset)> {
      match self {
        PairMaskTest::Either(either) => either.match_bytes_at(l_idx, l_byte, r_byte),
        PairMaskTest::Both(both) => both.match_bytes_at(l_idx, l_byte, r_byte),
      }
    }

    pub fn match_ends(
      &self, ends: (Option<(usize, u8)>, Option<u8>),
    ) -> Option<(usize, LotOffset)> {
      let (l_end, r_end) = ends;
      match self {
        PairMaskTest::Either(either) => either.match_ends(l_end, r_end),
        PairMaskTest::Both(both) => both.match_ends(l_end, r_end),
      }
    }
  }

  impl From<EEMask> for PairMaskTest<0> {
    #[inline]
    fn from(value: EEMask) -> Self {
      PairMaskTest::Either(value)
    }
  }

  impl<const N: usize> From<BEMask<N>> for PairMaskTest<N> {
    #[inline]
    fn from(value: BEMask<N>) -> Self {
      PairMaskTest::Both(value)
    }
  }

  #[cfg(test)]
  mod tests {
    use super::*;

    fn test_needle<const N: usize>(n: NMask<N>) {
      for i in 0..N {
        assert_eq!(
          Some((0, LotOffset(i as u8))),
          n.match_byte_at(0, 255u8 << i)
        )
      }
      for i in N..8 {
        assert_eq!(None, n.match_byte_at(0, 255u8 << i))
      }
    }

    #[test]
    fn needle_tests() {
      test_needle(N1);
      test_needle(N2);
      test_needle(N3);
      test_needle(N4);
      test_needle(N5);
      test_needle(N6);
      test_needle(N7);
      test_needle(N8);
    }

    #[test]
    fn ee_tests() {
      for (i, ee) in izip!((1..8).rev(), [EE1, EE2, EE3, EE4, EE5, EE6, EE7].iter()) {
        assert_eq!(
          Some((0, LotOffset(i))),
          ee.match_bytes_at(0, 255u8 << i, 255u8 >> i)
        );
        assert_eq!(Some((1, LotOffset(0))), ee.match_bytes_at(0, 0, 255u8 >> i));
        assert_eq!(None, ee.match_bytes_at(0, 0, 0));
      }
    }

    fn be_test_count<const N: usize>(expected: u32, mask: BEMask<N>) {
      for (l, r) in izip!(mask.0, mask.1) {
        assert_eq!(expected, l.count_ones() + r.count_ones());
      }
    }

    fn be_test_mask<const N: usize>(mask: BEMask<N>) {
      for (i, j) in izip!(8 - N..8, (8 - N..8).rev()) {
        //println!("Some - {:b} - {:b}", 255u8 << i, 255u8 >> j);
        assert_eq!(
          Some((0, LotOffset(i as u8))),
          mask.match_bytes_at(0, 255u8 << i, 255u8 >> j)
        );
        //println!("None - {:b} - {:b}", 255u8 >> i, 255u8 << j);
        assert_eq!(None, mask.match_bytes_at(0, 255u8 >> i, 255u8 << j));
        //println!("None - {:b} - {:b}", 255u8 << i, 255u8 << j);
        assert_eq!(None, mask.match_bytes_at(0, 255u8 << i, 255u8 << j));
      }
    }

    #[test]
    fn be_tests() {
      be_test_count(2, BE2);
      be_test_count(3, BE3);
      be_test_count(4, BE4);
      be_test_count(5, BE5);
      be_test_count(6, BE6);
      be_test_count(7, BE7);
      be_test_count(8, BE8);
      be_test_mask(BE2);
      be_test_mask(BE3);
      be_test_mask(BE4);
      be_test_mask(BE5);
      be_test_mask(BE6);
      be_test_mask(BE7);
      be_test_mask(BE8);
    }
  }
}

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

impl<T> LenTrait for T where T: AsRef<[u8]> {
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

impl<T> RangedIterator for T where T: AsRef<[u8]> {
  fn iterate_from(&self, midpoint: usize) -> impl Iterator<Item=u8> + Sized + ExactSizeIterator + DoubleEndedIterator {
    self.as_ref()[midpoint..].iter().copied()
  }

  fn iterate_to(&self, midpoint: usize) -> impl Iterator<Item=u8> + Sized + ExactSizeIterator + DoubleEndedIterator {
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
    SearchPattern {
      store,
      goal_lot,
    }
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

  pub fn needle_search<const N: usize>(&self, nmask: NMask<N>) -> Option<(usize, LotOffset)> {
    self
      .store
      .iterate_from(self.goal_lot)
      .enumerate()
      .map(|(idx, byte)| (idx + self.goal_lot, byte))
      .filter_map(|(idx, byte)| nmask.match_byte_at(idx, byte))
      .next()
  }

  pub fn needle_rsearch<const N: usize>(&self, nmask: NMask<N>) -> Option<(usize, LotOffset)> {
    self
      .store
      .iterate_to(self.goal_lot)
      .enumerate()
      .rev()
      .map(|(idx, byte)| (idx + self.goal_lot, byte))
      .filter_map(|(idx, byte)| nmask.match_byte_at(idx, byte))
      .next()
  }

  pub fn pair_search<const N: usize>(
    &self, pair_mask_test: PairMaskTest<N>,
  ) -> Option<(usize, LotOffset)> {
    self
      .store
      .iterate_from(self.goal_lot)
      .enumerate()
      .map(|(idx, byte)| (idx + self.goal_lot, byte))
      .tuple_windows()
      .map(|(((l_idx, l_byte), (_, r_byte)))| (l_idx, l_byte, r_byte))
      .filter_map(|(l_idx, l_byte, r_byte)| pair_mask_test.match_bytes_at(l_idx, l_byte, r_byte))
      .next()
  }

  pub fn pair_rsearch<const N: usize>(
    &self, pair_mask_test: PairMaskTest<N>,
  ) -> Option<(usize, LotOffset)> {
    self
      .store
      .iterate_to(self.goal_lot)
      .enumerate()
      .rev()
      .map(|(idx, byte)| (idx, byte))
      .tuple_windows()
      .map(|(((_, r_byte), (l_idx, l_byte)))| (l_idx, l_byte, r_byte))
      .filter_map(|(l_idx, l_byte, r_byte)| pair_mask_test.match_bytes_at(l_idx, l_byte, r_byte))
      .next()
  }

  pub fn boyer_moore_magiclen_search<const N: usize>(
    &self, free_bytes_len: usize, mask_test: PairMaskTest<N>,
  ) -> Option<(usize, LotOffset)> {
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

    let mut shift = self.goal_lot - free_bytes_len;

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
        return ends_match;
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
  ) -> Option<(usize, LotOffset)> {
    if self.store.len() == 0
      || free_bytes_len == 0
      || self.store.len() < free_bytes_len
      || self.store.len() < self.goal_lot
      || self.store.len() - self.goal_lot < free_bytes_len
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
        return ends_match;
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
  use crate::freelist::search::masks::BE8;
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
  pub fn test2() {
    let mut v = [0b1111_1110u8, 255, 255, 255, 0b0000_0001u8, 0, 0, 0, 0, 0, 0, 0 ,0 , 0, 0, 0 ];
    let midpoint = 3;
    let free_bytes_len = 3;
    for _ in 0..16 {
      let s = SearchPattern::new(&v, midpoint);
      let o = s.boyer_moore_magiclen_rsearch(free_bytes_len, BE8.into());
      println!("{:?}", o);
      v.rotate_right(1);
    }
  }
}
