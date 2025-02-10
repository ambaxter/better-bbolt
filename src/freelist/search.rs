use crate::freelist::search::masks::{EndMaskTest, GetLotOffset};
use bbolt_engine::common::ids::LotOffset;
use std::iter::{repeat_n, RepeatN};
use std::ops::Index;

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
      0b1111_1110u8,
    ],
  );

  // TODO: Would this be better as a trait?
  // Something to test later
  #[derive(Clone, Copy)]
  pub enum EndMaskTest<const N: usize> {
    Either(EEMask),
    Both(BEMask<N>),
  }

  impl EndMaskTest<0> {
    pub fn new_either(either: EEMask) -> EndMaskTest<0> {
      Self::Either(either)
    }
  }
  impl<const N: usize> EndMaskTest<N> {
    pub fn new_both(both: BEMask<N>) -> EndMaskTest<N> {
      Self::Both(both)
    }

    pub fn match_ends(
      &self, ends: (Option<(usize, u8)>, Option<u8>),
    ) -> Option<(usize, LotOffset)> {
      let (l_end, r_end) = ends;
      match self {
        EndMaskTest::Either(either) => either.match_ends(l_end, r_end),
        EndMaskTest::Both(both) => both.match_ends(l_end, r_end),
      }
    }
  }

  #[cfg(test)]
  mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn needle_tests() {
      let midpoint = 10;
      let v = vec![255; 16];
      for i in v[..midpoint].iter().enumerate().rev() {
        println!("{:?}", i);
      }
      println!("next");
      for i in v[midpoint..]
        .iter()
        .enumerate()
        .map(|(idx, d)| (idx + midpoint, d))
      {
        println!("{:?}", i);
      }
    }

    #[test]
    fn ee_tests() {
      let midpoint = 10;
      let v = vec![255; 16];
      for i in v[..midpoint]
        .iter()
        .enumerate()
        .rev()
        .tuple_windows::<(_, _)>()
        .map(|((_, r_byte), (l_idx, l_byte))| ((l_idx, l_byte), r_byte))
      {
        println!("{:?}", i);
      }
      println!("next");
      for i in v[midpoint..]
        .iter()
        .enumerate()
        .tuple_windows::<(_, _)>()
        .map(|((l_idx, l_byte), (r_idx, r_byte))| ((l_idx + midpoint, l_byte), r_byte))
      {
        println!("{:?}", i);
      }
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

impl LenTrait for Vec<u8> {
  #[inline]
  fn is_empty(&self) -> bool {
    self.is_empty()
  }

  #[inline]
  fn len(&self) -> usize {
    self.len()
  }
}

#[derive(Clone, Copy)]
pub struct SearchPattern<'a, T>
where
  T: ?Sized,
{
  store: &'a T,
  midpoint: usize,
}

impl<'a, T> SearchPattern<'a, T>
where
  T: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a T: IntoIterator<Item = &'a u8>,
  <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
{
  pub fn new(store: &'a T, midpoint: usize) -> SearchPattern<'a, T> {
    SearchPattern { store, midpoint }
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

  pub fn needle_search<const N: usize>(&self) -> Option<(usize, LotOffset)> {
    self.store[0..self.midpoint].iter().enumerate();

    unimplemented!()
  }

  pub fn boyer_moore_magiclen_search<const N: usize>(
    &self, free_bytes_len: usize, mask_test: EndMaskTest<N>,
  ) -> Option<(usize, LotOffset)> {
    if self.store.len() == 0
      || free_bytes_len == 0
      || self.store.len() < free_bytes_len
      || self.store.len() < self.midpoint
      || self.store.len() - self.midpoint < free_bytes_len
    {
      return None;
    }

    let pattern_len_dec = free_bytes_len - 1;

    let last_pattern_char = u8::MAX;

    let mut shift = self.midpoint - free_bytes_len;

    let end_index = self.store.len() - free_bytes_len;

    'outer: loop {
      for (i, pc) in Self::repeat_iter(free_bytes_len).enumerate().rev() {
        if self.store[shift + i] != pc {
          let p = shift + free_bytes_len;
          if p == self.store.len() {
            break 'outer;
          }
          shift +=
            Self::bad_shift_index(free_bytes_len, self.store[shift + pattern_len_dec]).max({
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
          if end_match.is_some() {
            shift += 1;
          }
        }
        end_match
      };
      if ends_match.is_some() {
        return ends_match;
      }
      shift += 1;
      continue;
    }

    None
  }
}

#[cfg(test)]
mod tests {
  use crate::freelist::search::masks::{EndMaskTest, EE1};
  use crate::freelist::search::SearchPattern;

  #[test]
  pub fn test() {
    let mask_test = EndMaskTest::new_either(EE1);
    let v = vec![u8::MAX; 16];
    let midpoint = 8usize;
    let free_bytes_len = 3;
    let s = SearchPattern::new(&v, midpoint);
    let o = s.boyer_moore_magiclen_search(free_bytes_len, mask_test);
    println!("{:?}", o);
  }
}
