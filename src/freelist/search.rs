use crate::freelist::search::masks::{EndMaskTest, GetLotOffset};
use bbolt_engine::common::ids::{LotOffset};
use std::iter::{repeat_n, RepeatN};
use std::ops::Index;

pub mod masks {
  use bbolt_engine::common::ids::LotOffset;

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
  pub const N1: [u8; 8] = [
    0b0000_0001u8,
    0b0000_0010u8,
    0b0000_0100u8,
    0b0000_1000u8,
    0b0001_0000u8,
    0b0010_0000u8,
    0b0100_0000u8,
    0b1000_0000u8,
  ];

  pub const N2: [u8; 7] = [
    0b0000_0011u8,
    0b0000_0110u8,
    0b0000_1100u8,
    0b0001_1000u8,
    0b0011_0000u8,
    0b0110_0000u8,
    0b1100_0000u8,
  ];

  pub const N3: [u8; 6] = [
    0b0000_0111u8,
    0b0000_1110u8,
    0b0001_1100u8,
    0b0011_1000u8,
    0b0111_0000u8,
    0b1110_0000u8,
  ];

  pub const N4: [u8; 5] = [
    0b0000_1111u8,
    0b0001_1110u8,
    0b0011_1100u8,
    0b0111_1000u8,
    0b1111_0000u8,
  ];

  pub const N5: [u8; 4] = [0b0001_1111u8, 0b0011_1110u8, 0b0111_1100u8, 0b1111_1000u8];

  pub const N6: [u8; 3] = [0b0011_1111u8, 0b0111_1110u8, 0b1111_1100u8];

  // I’ve killed worse than you on my way to real problems - Commander Shepard
  pub const N7: [u8; 2] = [0b0111_1111u8, 0b1111_1110u8];

  pub const N8: [u8; 1] = [0b1111_1111u8];

  pub(crate) fn match_needle<const N: usize>(masks: [u8; N]) -> impl Fn(u8) -> Option<LotOffset> {
    move |byte| {
      masks
        .iter()
        .enumerate()
        .filter_map(|(idx, &mask)| {
          if (byte & mask) == mask {
            Some(LotOffset(idx as u8))
          } else {
            None
          }
        })
        .next()
    }
  }

  // either end mask
  pub const EE1: (u8, u8) = (0b1000_0000u8, 0b0000_0001u8);
  pub const EE2: (u8, u8) = (0b1100_0000u8, 0b0000_0011u8);
  pub const EE3: (u8, u8) = (0b1110_0000u8, 0b0000_0111u8);
  pub const EE4: (u8, u8) = (0b1111_0000u8, 0b0000_1111u8);
  pub const EE5: (u8, u8) = (0b1111_1000u8, 0b0001_1111u8);
  pub const EE6: (u8, u8) = (0b1111_1100u8, 0b0011_1111u8);
  pub const EE7: (u8, u8) = (0b1111_1110u8, 0b0111_1111u8);

  pub const BE2: [(u8, u8); 1] = [(0b1000_0000u8, 0b0000_0001u8)];

  pub const BE3: [(u8, u8); 2] = [
    (0b1100_0000u8, 0b0000_0001u8),
    (0b1000_0000u8, 0b0000_0011u8),
  ];

  pub const BE4: [(u8, u8); 3] = [
    (0b1110_0000u8, 0b0000_0001u8),
    (0b1100_0000u8, 0b0000_0011u8),
    (0b1000_0000u8, 0b0000_0111u8),
  ];

  pub const BE5: [(u8, u8); 4] = [
    (0b1111_0000u8, 0b0000_0001u8),
    (0b1110_0000u8, 0b0000_0011u8),
    (0b1100_0000u8, 0b0000_0111u8),
    (0b1000_0000u8, 0b0000_1111u8),
  ];

  pub const BE6: [(u8, u8); 5] = [
    (0b1111_1000u8, 0b0000_0001u8),
    (0b1111_0000u8, 0b0000_0011u8),
    (0b1110_0000u8, 0b0000_0111u8),
    (0b1100_0000u8, 0b0000_1111u8),
    (0b1000_0000u8, 0b0001_1111u8),
  ];

  pub const BE7: [(u8, u8); 6] = [
    (0b1111_1100u8, 0b0000_0001u8),
    (0b1111_1000u8, 0b0000_0011u8),
    (0b1111_0000u8, 0b0000_0111u8),
    (0b1110_0000u8, 0b0000_1111u8),
    (0b1100_0000u8, 0b0001_1111u8),
    (0b1000_0000u8, 0b0011_1111u8),
  ];

  pub const BE8: [(u8, u8); 7] = [
    (0b1111_1110u8, 0b0000_0001u8),
    (0b1111_1100u8, 0b0000_0011u8),
    (0b1111_1000u8, 0b0000_0111u8),
    (0b1111_0000u8, 0b0000_1111u8),
    (0b1110_0000u8, 0b0001_1111u8),
    (0b1100_0000u8, 0b0011_1111u8),
    (0b1000_0000u8, 0b0111_1111u8),
  ];


  #[derive(Clone, Copy)]
  pub enum EndMaskTest<const N: usize> {
    Either(u8, u8),
    Both([(u8, u8); N]),
  }

  impl EndMaskTest<0> {
    pub fn new_either(either: (u8, u8)) -> EndMaskTest<0> {
      Self::Either(either.0, either.1)
    }
  }
  impl<const N: usize> EndMaskTest<N> {
    pub fn new_both(masks: [(u8, u8); N]) -> EndMaskTest<N> {
      Self::Both(masks)
    }

    pub fn find_match(&self, ends: (Option<(usize, u8)>, Option<u8>)) -> Option<(usize, LotOffset)> {
      let (l, r) = ends;
      match (l, r, self) {
        (Some((l_idx, l_byte)), _, EndMaskTest::Either(l_mask, _)) if *l_mask & l_byte == *l_mask => {
          Some((l_idx, l_mask.high_offset()))
        }
        (Some((l_idx, _)), Some(r_byte), EndMaskTest::Either(_, r_mask))
        if *r_mask & r_byte == *r_mask =>
          {
            Some((l_idx + 1, LotOffset(0)))
          }
        (Some((l_idx, l_byte)), Some(r_byte), EndMaskTest::Both(masks)) => masks
          .iter()
          .enumerate()
          .filter_map(|(idx, &(l_mask, r_mask))| {
            if (l_byte & l_mask) == l_mask && (r_byte & r_mask) == r_mask {
              Some(l_mask.high_offset())
            } else {
              None
            }
          })
          .next()
          .map(|offset| (l_idx, offset)),
        _ => None,
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
  pattern_len: usize,
}

impl<'a, T> SearchPattern<'a, T>
where
  T: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a T: IntoIterator<Item = &'a u8>,
  <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
{
  pub fn new(store: &'a T, midpoint: usize, pattern_len: usize) -> SearchPattern<'a, T> {
    SearchPattern {
      store,
      midpoint,
      pattern_len,
    }
  }

  fn bad_shift_index(&self, index: u8) -> usize {
    if index == u8::MAX {
      0
    } else {
      self.pattern_len
    }
  }

  fn repeat_iter(&self) -> RepeatN<u8> {
    repeat_n(u8::MAX, self.pattern_len)
  }

  fn index_pattern(&self, index: usize) -> u8 {
    assert!(index < self.pattern_len);
    u8::MAX
  }

  fn get_ends(&self, shift_idx: usize) -> (Option<(usize, u8)>, Option<u8>) {
    let r_idx = shift_idx + self.pattern_len;
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

  pub fn search<const N: usize>(&self, mask_test: EndMaskTest<N>) -> Option<(usize, LotOffset)> {
    if self.store.len() == 0
      || self.pattern_len == 0
      || self.store.len() < self.pattern_len
      || self.store.len() < self.midpoint
      || self.store.len() - self.midpoint < self.pattern_len
    {
      return None;
    }

    let pattern_len_dec = self.pattern_len - 1;

    let last_pattern_char = u8::MAX;

    let mut shift = self.midpoint - self.pattern_len;

    let end_index = self.store.len() - self.pattern_len;

    'outer: loop {
      for (i, pc) in self.repeat_iter().enumerate().rev() {
        if self.store[shift + i] != pc {
          let p = shift + self.pattern_len;
          if p == self.store.len() {
            break 'outer;
          }
          shift += self
            .bad_shift_index(self.store[shift + pattern_len_dec])
            .max({
              let c = self.store[p];
              if c == last_pattern_char {
                1
              } else {
                self.bad_shift_index(c) + 1
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
        let mut end_match = mask_test.find_match(self.get_ends(shift));
        if end_match.is_none() && in_run {
          end_match = mask_test.find_match(self.get_ends(shift + 1));
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
  use crate::freelist::search::masks::EE2;
  use crate::freelist::search::{EndMaskTest, SearchPattern};

  #[test]
  pub fn test() {
    let mask_test = EndMaskTest::new_either(EE2);
    let v = vec![u8::MAX; 16];
    let midpoint = 8usize;
    let pattern_len = 3;
    let s = SearchPattern::new(&v, midpoint, pattern_len);
    let o = s.search(mask_test);
    println!("{:?}", o);
  }
}
