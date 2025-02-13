use bbolt_engine::common::ids::LotOffset;
use itertools::izip;

pub trait GetLotOffset {
  /// On left pair mask checks we need to know which
  /// bit starts the run. Currently, it's `8 - byte.count_ones()`
  /// I'm sure someone knows a better way
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
  pub fn match_bytes_at(&self, l_idx: usize, l_byte: u8, r_byte: u8) -> Option<(usize, LotOffset)> {
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
pub const EE8: EEMask = EEMask(0b1111_1111u8, 0b1111_1111u8);

#[derive(Debug, Copy, Clone)]
pub struct BEMask<const N: usize>(pub [u8; N], [u8; N]);

impl<const N: usize> BEMask<N> {
  pub fn match_bytes_at(&self, l_idx: usize, l_byte: u8, r_byte: u8) -> Option<(usize, LotOffset)> {
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

pub const BE9: BEMask<6> = BEMask(
  [
    0b1111_1110u8,
    0b1111_1100u8,
    0b1111_1000u8,
    0b1111_0000u8,
    0b1110_0000u8,
    0b1100_0000u8,
  ],
  [
    0b0000_0011u8,
    0b0000_0111u8,
    0b0000_1111u8,
    0b0001_1111u8,
    0b0011_1111u8,
    0b0111_1111u8,
  ],
);

pub const BE10: BEMask<5> = BEMask(
  [
    0b1111_1110u8,
    0b1111_1100u8,
    0b1111_1000u8,
    0b1111_0000u8,
    0b1110_0000u8,
  ],
  [
    0b0000_0111u8,
    0b0000_1111u8,
    0b0001_1111u8,
    0b0011_1111u8,
    0b0111_1111u8,
  ],
);

pub const BE11: BEMask<4> = BEMask(
  [0b1111_1110u8, 0b1111_1100u8, 0b1111_1000u8, 0b1111_0000u8],
  [0b0000_1111u8, 0b0001_1111u8, 0b0011_1111u8, 0b0111_1111u8],
);

pub const BE12: BEMask<3> = BEMask(
  [0b1111_1110u8, 0b1111_1100u8, 0b1111_1000u8],
  [0b0001_1111u8, 0b0011_1111u8, 0b0111_1111u8],
);

pub const BE13: BEMask<2> = BEMask(
  [0b1111_1110u8, 0b1111_1100u8],
  [0b0011_1111u8, 0b0111_1111u8],
);

pub const BE14: BEMask<1> = BEMask([0b1111_1110u8], [0b0111_1111u8]);

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

  pub fn match_bytes_at(&self, l_idx: usize, l_byte: u8, r_byte: u8) -> Option<(usize, LotOffset)> {
    match self {
      PairMaskTest::Either(either) => either.match_bytes_at(l_idx, l_byte, r_byte),
      PairMaskTest::Both(both) => both.match_bytes_at(l_idx, l_byte, r_byte),
    }
  }

  pub fn match_ends(&self, ends: (Option<(usize, u8)>, Option<u8>)) -> Option<(usize, LotOffset)> {
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
    be_test_count(9, BE9);
    be_test_count(10, BE10);
    be_test_count(11, BE11);
    be_test_count(12, BE12);
    be_test_count(13, BE13);
    be_test_count(14, BE14);
    be_test_mask(BE2);
    be_test_mask(BE3);
    be_test_mask(BE4);
    be_test_mask(BE5);
    be_test_mask(BE6);
    be_test_mask(BE7);
    be_test_mask(BE8);
  }
}
