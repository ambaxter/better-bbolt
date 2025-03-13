pub trait BitSet {
  fn clear(&mut self);
  fn get_bit(&self, bit: u8) -> bool;
  fn clear_bit(&mut self, bit: u8);
  fn clear_mask(&mut self, mask: u8);
  fn set_bit(&mut self, bit: u8);
  fn set_mask(&mut self, mask: u8);
  fn toggle_bit(&mut self, bit: u8);
}

impl BitSet for u8 {
  #[inline]
  fn clear(&mut self) {
    *self = 0;
  }

  #[inline]
  fn get_bit(&self, bit: u8) -> bool {
    debug_assert!(bit < 8);
    *self & (1 << bit) != 0
  }

  #[inline]
  fn clear_bit(&mut self, bit: u8) {
    debug_assert!(bit < 8);
    *self &= !(1 << bit);
  }

  #[inline]
  fn clear_mask(&mut self, mask: u8) {
    debug_assert!(*self & mask == mask);
    *self &= !mask;
  }

  #[inline]
  fn set_bit(&mut self, bit: u8) {
    debug_assert!(bit < 8);
    *self |= 1 << bit;
  }

  #[inline]
  fn set_mask(&mut self, mask: u8) {
    debug_assert!(*self & mask == 0);
    *self |= mask;
  }

  #[inline]
  fn toggle_bit(&mut self, bit: u8) {
    debug_assert!(bit < 8);
    *self ^= 1 << bit;
  }
}

#[cfg(test)]
pub mod tests {
  use crate::common::bitset::BitSet;

  #[test]
  pub fn test_bitset() {
    let mut i = u8::MAX;
    println!("{:b}", i);
    i.clear_mask(0b0011_1100u8);
    println!("{:b}", i);
    i.set_mask(0b0011_1100u8);
    println!("{:b}", i);
  }
}
