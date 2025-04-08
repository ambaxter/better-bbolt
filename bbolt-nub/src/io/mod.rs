use std::marker::PhantomData;
use std::ops::Deref;

pub mod backends;
pub mod bytes;
pub mod ops;
pub mod pages;
pub mod transmogrify;

#[derive(Debug, Copy, Clone, Default)]
pub struct TxSlot<'tx> {
  tx: PhantomData<&'tx [u8]>,
}

pub trait AsCopiedIter {
  type Iter: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator;
  fn as_copied_iter(&self) -> Self::Iter;
}
