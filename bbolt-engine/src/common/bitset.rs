pub trait BitSet {
  fn clear(&mut self);
  fn get(&self, bit: u8) -> bool;
  fn unset(&mut self, bit: u8);
  fn set(&mut self, bit: u8);
  fn toggle(&mut self, bit: u8);
}

impl BitSet for u8 {
  #[inline]
  fn clear(&mut self) {
    *self = 0;
  }

  #[inline]
  fn get(&self, bit: u8) -> bool {
    debug_assert!(bit < 8);
    *self & (1 << bit) != 0
  }

  #[inline]
  fn unset(&mut self, bit: u8) {
    debug_assert!(bit < 8);
    *self &= !(1 << bit);
  }

  #[inline]
  fn set(&mut self, bit: u8) {
    debug_assert!(bit < 8);
    *self |= 1 << bit;
  }

  #[inline]
  fn toggle(&mut self, bit: u8) {
    debug_assert!(bit < 8);
    *self ^= 1 << bit;
  }
}
