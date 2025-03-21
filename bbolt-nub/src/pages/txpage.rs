pub trait IntoCopiedIterator {
  fn iter_copied(&self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator;
}

impl<'a> IntoCopiedIterator for &'a [u8] {
  fn iter_copied(&self) -> impl Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator {
    self.iter().cloned()
  }
}