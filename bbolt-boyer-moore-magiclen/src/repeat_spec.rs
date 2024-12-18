use crate::byte::LenTrait;
use alloc::fmt::Debug;
use core::iter::{repeat_n, RepeatN};
use core::ops::Index;
use smallvec::{smallvec, SmallVec};

#[derive(Debug, Clone, Copy)]
pub struct BMRepeatPattern {
  byte: u8,
  len: usize,
}

impl BMRepeatPattern {
  #[inline]
  pub fn new(byte: u8, len: usize) -> BMRepeatPattern {
    BMRepeatPattern { byte, len }
  }
}

impl LenTrait for BMRepeatPattern {
  #[inline]
  fn is_empty(&self) -> bool {
    self.len == 0
  }

  #[inline]
  fn len(&self) -> usize {
    self.len
  }
}

impl Index<usize> for BMRepeatPattern {
  type Output = u8;
  #[inline]
  fn index(&self, idx: usize) -> &u8 {
    assert!(idx < self.len);
    &self.byte
  }
}

impl<'a> IntoIterator for &'a BMRepeatPattern {
  type Item = &'a u8;
  type IntoIter = RepeatN<&'a u8>;

  fn into_iter(self) -> Self::IntoIter {
    repeat_n(&self.byte, self.len)
  }
}

#[derive(Debug, Clone, Copy)]
pub struct BMRepeatBadCharShiftMap {
  pattern: BMRepeatPattern,
}

impl BMRepeatBadCharShiftMap {
  #[inline]
  pub fn new(pattern: BMRepeatPattern) -> Self {
    Self { pattern }
  }
}

impl Index<u8> for BMRepeatBadCharShiftMap {
  type Output = usize;

  fn index(&self, index: u8) -> &Self::Output {
    if self.pattern.byte == index {
      &0
    } else {
      &self.pattern.len
    }
  }
}

// TODO BM

/// Using Boyer-Moore-MagicLen to search byte sub-sequences in any byte sequence, including self-synchronizing string encoding data such as UTF-8.
/// This one specialized for a number of repeating characters
#[derive(Debug)]
pub struct BMRepeat {
  bad_char_shift_map: BMRepeatBadCharShiftMap,
  pattern: BMRepeatPattern,
}

impl BMRepeat {
  pub fn new(byte: u8, len: usize) -> BMRepeat {
    let pattern = BMRepeatPattern::new(byte, len);
    let bad_char_shift_map = BMRepeatBadCharShiftMap { pattern };
    BMRepeat {
      bad_char_shift_map,
      pattern,
    }
  }
}

// TODO Find

impl BMRepeat {
  /// Find and return the position of the first matched sub-sequence in any text (the haystack).
  ///
  /// ```
  /// use bbolt_boyer_moore_magiclen::BMByte;
  ///
  /// let bmb = BMByte::from("oocoo").unwrap();
  ///
  /// assert_eq!(Some(1), bmb.find_first_in("coocoocoocoo"));
  /// ```
  pub fn find_first_in<'a, T, F>(&'a self, text: &'a T, e: FindEndMasks<F>) -> Option<FindResults>
  where
    T: Index<usize, Output = u8> + LenTrait + ?Sized,
    &'a T: IntoIterator<Item = &'a u8>,
    <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(u8, u8) -> Option<(u8, u8)>,
  {
    // TODO: Why do I need to explicitly set the types? Shouldn't the compiler figure it out on its own?
    // https://github.com/rust-lang/rust/issues/134387
    find_spec::<T, BMRepeatPattern, F>(text, &self.pattern, &self.bad_char_shift_map, 1, e)
      .first()
      .copied()
  }

  /// Find and return the positions of matched sub-sequences in any text (the haystack) but not including the overlap. If the `limit` is set to `0`, all sub-sequences will be found.
  ///
  /// ```
  /// use bbolt_boyer_moore_magiclen::BMByte;
  ///
  /// let bmb = BMByte::from("oocoo").unwrap();
  ///
  /// assert_eq!(vec![1], bmb.find_in("coocoocoocoo", 1));
  /// ```
  pub fn find_in<'a, T, F>(
    &'a self, text: &'a T, limit: usize, e: FindEndMasks<F>,
  ) -> SmallVec<[FindResults; 1]>
  where
    T: Index<usize, Output = u8> + LenTrait + ?Sized,
    &'a T: IntoIterator<Item = &'a u8>,
    <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(u8, u8) -> Option<(u8, u8)>,
  {
    find_spec::<T, BMRepeatPattern, F>(text, &self.pattern, &self.bad_char_shift_map, limit, e)
  }
}

impl BMRepeat {
  /// Find and return the position of the first matched sub-sequence in any text (the haystack) from its tail to its head.
  ///
  /// ```
  /// use bbolt_boyer_moore_magiclen::BMByte;
  ///
  /// let bmb = BMByte::from("oocoo").unwrap();
  ///
  /// assert_eq!(Some(7), bmb.rfind_first_in("coocoocoocoo"));
  /// ```
  pub fn rfind_first_in<'a, T, F>(&'a self, text: &'a T, e: FindEndMasks<F>) -> Option<FindResults>
  where
    T: Index<usize, Output = u8> + LenTrait + ?Sized,
    &'a T: IntoIterator<Item = &'a u8>,
    <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(u8, u8) -> Option<(u8, u8)>,
  {
    rfind_spec::<T, BMRepeatPattern, F>(text, &self.pattern, &self.bad_char_shift_map, 1, e)
      .first()
      .copied()
  }

  /// Find and return the positions of matched sub-sequences in any text (the haystack) but not including the overlap from its tail to its head. If the `limit` is set to `0`, all sub-sequences will be found.
  ///
  /// ```
  /// use bbolt_boyer_moore_magiclen::BMByte;
  ///
  /// let bmb = BMByte::from("oocoo").unwrap();
  ///
  /// assert_eq!(vec![7], bmb.rfind_in("coocoocoocoo", 1));
  /// ```
  pub fn rfind_in<'a, T, F>(
    &'a self, text: &'a T, limit: usize, e: FindEndMasks<F>,
  ) -> SmallVec<[FindResults; 1]>
  where
    T: Index<usize, Output = u8> + LenTrait + ?Sized,
    &'a T: IntoIterator<Item = &'a u8>,
    <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(u8, u8) -> Option<(u8, u8)>,
  {
    rfind_spec::<T, BMRepeatPattern, F>(text, &self.pattern, &self.bad_char_shift_map, limit, e)
  }
}

#[derive(Debug, Copy, Clone)]
pub enum EndMasks {
  L(u8),
  R(u8),
  BOTH(u8, u8),
}

#[derive(Debug, Copy, Clone)]
pub struct FindResults {
  pub index: usize,
  end_masks: EndMasks,
}

pub enum FindEndMasks<F>
where
  F: Fn(u8, u8) -> Option<(u8, u8)>,
{
  Either(u8, u8),
  Both(F),
}

impl<F> FindEndMasks<F>
where
  F: Fn(u8, u8) -> Option<(u8, u8)>,
{
  pub fn find_match(&self, l: Option<u8>, r: Option<u8>) -> Option<EndMasks> {
    match (l, r, self) {
      (Some(l), _, FindEndMasks::Either(mask_l, _)) if *mask_l & l == *mask_l => {
        Some(EndMasks::L(*mask_l))
      }
      (_, Some(r), FindEndMasks::Either(_, mask_r)) if *mask_r & r == *mask_r => {
        Some(EndMasks::R(*mask_r))
      }
      (Some(l), Some(r), FindEndMasks::Both(both_masks)) => {
        if let Some((mask_l, mask_r)) = both_masks(l, r) {
          Some(EndMasks::BOTH(mask_l, mask_r))
        } else {
          None
        }
      }
      _ => None,
    }
  }
}

pub fn find_spec<'a, TT: 'a, TP: 'a, F>(
  text: &'a TT, pattern: &'a TP, bad_char_shift_map: &BMRepeatBadCharShiftMap, limit: usize,
  e: FindEndMasks<F>,
) -> SmallVec<[FindResults; 1]>
where
  TT: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TT: IntoIterator<Item = &'a u8>,
  <&'a TT as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  TP: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TP: IntoIterator<Item = &'a u8>,
  <&'a TP as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  F: Fn(u8, u8) -> Option<(u8, u8)>,
{
  let text_len = text.len();
  let pattern_len = pattern.len();

  if text_len == 0 || pattern_len == 0 || text_len < pattern_len {
    return smallvec![];
  }

  let pattern_len_dec = pattern_len - 1;

  let last_pattern_char = pattern[pattern_len_dec];

  let mut shift = 0;

  let end_index = text_len - pattern_len;

  let mut result = smallvec![];

  'outer: loop {
    for (i, pc) in pattern.into_iter().copied().enumerate().rev() {
      if text[shift + i] != pc {
        let p = shift + pattern_len;
        if p == text_len {
          break 'outer;
        }
        shift += bad_char_shift_map[text[shift + pattern_len_dec]].max({
          let c = text[p];

          if c == last_pattern_char {
            1
          } else {
            bad_char_shift_map[c] + 1
          }
        });
        if shift > end_index {
          break 'outer;
        }
        continue 'outer;
      }
    }
    // test to see if the ends match
    let ends_match = {
      let r = if shift + pattern_len < text_len - 1 {
        Some(text[shift + pattern_len])
      } else {
        None
      };
      let l = if shift > 0 {
        Some(text[shift - 1])
      } else {
        None
      };
      e.find_match(l, r)
    };
    if let Some(end_masks) = ends_match {
      let r = FindResults {
        index: shift,
        end_masks,
      };
      result.push(r);
    } else {
      shift += 1;
      continue;
    }

    if shift == end_index {
      break;
    }

    if result.len() == limit {
      break;
    }

    shift += pattern_len;
    if shift > end_index {
      break;
    }
  }

  result
}

pub fn rfind_spec<'a, TT: 'a, TP: 'a, F>(
  text: &'a TT, pattern: &'a TP, bad_char_shift_map: &BMRepeatBadCharShiftMap, limit: usize,
  e: FindEndMasks<F>,
) -> SmallVec<[FindResults; 1]>
where
  TT: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TT: IntoIterator<Item = &'a u8>,
  <&'a TT as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  TP: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TP: IntoIterator<Item = &'a u8>,
  <&'a TP as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  F: Fn(u8, u8) -> Option<(u8, u8)>,
{
  let text_len = text.len();
  let pattern_len = pattern.len();

  if text_len == 0 || pattern_len == 0 || text_len < pattern_len {
    return smallvec![];
  }

  let pattern_len_dec = pattern_len - 1;

  let first_pattern_char = pattern[0];

  let mut shift = text_len - 1;

  let start_index = pattern_len_dec;

  let mut result = smallvec![];

  'outer: loop {
    for (i, pc) in pattern.into_iter().copied().enumerate() {
      if text[shift - pattern_len_dec + i] != pc {
        if shift < pattern_len {
          break 'outer;
        }
        let s = bad_char_shift_map[text[shift - pattern_len_dec]].max({
          let c = text[shift - pattern_len];

          if c == first_pattern_char {
            1
          } else {
            bad_char_shift_map[c] + 1
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
    // test to see if the ends match
    let ends_match = {
      let r = if shift < text_len - 1 {
        Some(text[shift + 1])
      } else {
        None
      };
      let l = if shift > pattern_len {
        Some(text[shift - pattern_len])
      } else {
        None
      };
      e.find_match(l, r)
    };
    if let Some(end_masks) = ends_match {
      let r = FindResults {
        index: shift,
        end_masks,
      };
      result.push(r);
    } else {
      if shift > 0 {
        shift -= 1;
        continue;
      }
    }

    if shift <= start_index {
      break;
    }

    if result.len() == limit {
      break;
    }

    shift -= pattern_len;
    if shift < start_index {
      break;
    }
  }

  result
}
