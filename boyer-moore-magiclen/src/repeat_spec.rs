use crate::byte::LenTrait;
use alloc::fmt::Debug;
use core::iter::{repeat_n, RepeatN};
use core::ops::Index;

// TODO: every N skips, test if we should keep going?
// if there is not a result keep going
// else if the current shift's 16 Mb quartile is < result's quartile, keep going

// if match and there is no match, use that
// if match and match's quartile is < result's quartile, use that
// if match and match's quartile is == result's quartile, use the one with the smaller shift

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
  /// use boyer_moore_magiclen::BMByte;
  ///
  /// let bmb = BMByte::from("oocoo").unwrap();
  ///
  /// assert_eq!(Some(1), bmb.find_first_in("coocoocoocoo"));
  /// ```
  pub fn find_first_in<'a, T, F, BF>(
    &'a self, text: &'a T, find_end_masks: FindEndMasks<F>, break_here: BF,
  ) -> Option<FindResults>
  where
    T: Index<usize, Output = u8> + LenTrait + ?Sized,
    &'a T: IntoIterator<Item = &'a u8>,
    <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(u8, u8) -> Option<(u8, u8)>,
    BF: Fn(usize) -> bool,
  {
    // TODO: Why do I need to explicitly set the types? Shouldn't the compiler figure it out on its own?
    // https://github.com/rust-lang/rust/issues/134387
    find_first_spec::<T, BMRepeatPattern, F, BF>(
      text,
      &self.pattern,
      &self.bad_char_shift_map,
      find_end_masks,
      break_here,
    )
  }
}

impl BMRepeat {
  /// Find and return the position of the first matched sub-sequence in any text (the haystack) from its tail to its head.
  ///
  /// ```
  /// use boyer_moore_magiclen::BMByte;
  ///
  /// let bmb = BMByte::from("oocoo").unwrap();
  ///
  /// assert_eq!(Some(7), bmb.rfind_first_in("coocoocoocoo"));
  /// ```
  pub fn rfind_first_in<'a, T, F, BF>(
    &'a self, text: &'a T, find_end_masks: FindEndMasks<F>, break_here: BF,
  ) -> Option<FindResults>
  where
    T: Index<usize, Output = u8> + LenTrait + ?Sized,
    &'a T: IntoIterator<Item = &'a u8>,
    <&'a T as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
    F: Fn(u8, u8) -> Option<(u8, u8)>,
    BF: Fn(usize) -> bool,
  {
    rfind_first_spec::<T, BMRepeatPattern, F, BF>(
      text,
      &self.pattern,
      &self.bad_char_shift_map,
      find_end_masks,
      break_here,
    )
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
  pub fn find_match(&self, ends: (Option<u8>, Option<u8>)) -> Option<EndMasks> {
    let (l, r) = ends;
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

pub fn find_first_spec<'a, TT: 'a, TP: 'a, F, BF>(
  text: &'a TT, pattern: &'a TP, bad_char_shift_map: &BMRepeatBadCharShiftMap,
  find_end_masks: FindEndMasks<F>, break_here: BF,
) -> Option<FindResults>
where
  TT: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TT: IntoIterator<Item = &'a u8>,
  <&'a TT as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  TP: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TP: IntoIterator<Item = &'a u8>,
  <&'a TP as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  F: Fn(u8, u8) -> Option<(u8, u8)>,
  BF: Fn(usize) -> bool,
{
  let text_len = text.len();
  let pattern_len = pattern.len();

  if text_len == 0 || pattern_len == 0 || text_len < pattern_len {
    return None;
  }

  let pattern_len_dec = pattern_len - 1;

  let last_pattern_char = pattern[pattern_len_dec];

  let mut shift = 0;

  let end_index = text_len - pattern_len;

  let mut shift_counts = 1usize;

  'outer: loop {
    if shift_counts % 256 == 0 && break_here(shift) {
      return None;
    }
    shift_counts += 1;
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
    let mut in_run = false;
    while shift > 0 && text[shift - 1] == u8::MAX {
      in_run = true;
      shift -= 1;
    }
    // test to see if the ends match
    let ends_fn = move |shift_idx| {
      let r = if shift_idx + pattern_len < text_len - 1 {
        Some(text[shift_idx + pattern_len])
      } else {
        None
      };
      let l = if shift_idx > 0 {
        Some(text[shift_idx - 1])
      } else {
        None
      };
      (l, r)
    };

    let ends_match = {
      let mut end_match = find_end_masks.find_match(ends_fn(shift));
      if end_match.is_none() && in_run {
        end_match = find_end_masks.find_match(ends_fn(shift + 1));
        if end_match.is_some() {
          shift += 1;
        }
      }
      end_match
    };
    if let Some(end_masks) = ends_match {
      let r = FindResults {
        index: shift,
        end_masks,
      };
      return Some(r);
    } else {
      shift += 1;
      continue;
    }
  }
  None
}

pub fn rfind_first_spec<'a, TT: 'a, TP: 'a, F, BF>(
  text: &'a TT, pattern: &'a TP, bad_char_shift_map: &BMRepeatBadCharShiftMap,
  find_end_masks: FindEndMasks<F>, break_here: BF,
) -> Option<FindResults>
where
  TT: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TT: IntoIterator<Item = &'a u8>,
  <&'a TT as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  TP: Index<usize, Output = u8> + LenTrait + ?Sized,
  &'a TP: IntoIterator<Item = &'a u8>,
  <&'a TP as IntoIterator>::IntoIter: Sized + DoubleEndedIterator + ExactSizeIterator,
  F: Fn(u8, u8) -> Option<(u8, u8)>,
  BF: Fn(usize) -> bool,
{
  let text_len = text.len();
  let pattern_len = pattern.len();

  if text_len == 0 || pattern_len == 0 || text_len < pattern_len {
    return None;
  }

  let pattern_len_dec = pattern_len - 1;

  let first_pattern_char = pattern[0];

  let mut shift = text_len - 1;

  let start_index = pattern_len_dec;

  let mut shift_counts = 1usize;

  'outer: loop {
    if shift_counts % 256 == 0 && break_here(shift) {
      return None;
    }
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
    let mut in_run = false;
    while shift - pattern_len > 0 && text[shift - pattern_len] == u8::MAX {
      in_run = true;
      shift -= 1;
    }
    // test to see if the ends match
    let ends_fn = move |shift_idx| {
      let r = if shift_idx < text_len - 1 {
        Some(text[shift_idx + 1])
      } else {
        None
      };
      let l = if shift_idx > pattern_len {
        Some(text[shift_idx - pattern_len])
      } else {
        None
      };
      (l, r)
    };
    let ends_match = {
      let mut ends_match = find_end_masks.find_match(ends_fn(shift));
      if ends_match.is_none() && in_run {
        ends_match = find_end_masks.find_match(ends_fn(shift + 1));
        if ends_match.is_some() {
          shift += 1;
        }
      }
      ends_match
    };
    if let Some(end_masks) = ends_match {
      let r = FindResults {
        index: shift,
        end_masks,
      };
      return Some(r);
    } else {
      if shift > 0 {
        shift -= 1;
        continue;
      }
    }

    if shift <= start_index {
      break;
    }

    shift -= pattern_len;
    if shift < start_index {
      break;
    }
  }

  None
}
