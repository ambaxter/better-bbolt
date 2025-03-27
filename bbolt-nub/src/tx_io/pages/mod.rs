use std::marker::PhantomData;
use std::ops::Deref;

pub mod kv;

pub mod lazy_page;

pub mod ref_page;
pub mod shared_page;

#[derive(Debug, Copy, Clone, Default)]
pub struct TxSlot<'tx> {
  tx: PhantomData<&'tx [u8]>,
}

pub trait AsCopiedIter {
  type Iter: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator;
  fn as_copied_iter(&self) -> Self::Iter;
}

pub trait IOBytes: Deref<Target = [u8]> + AsRef<[u8]> + Clone + Sized {}

pub trait TxBytes<'tx>: Deref<Target = [u8]> + AsRef<[u8]> + Clone + Sized {}

pub trait FromIO<'tx, T: IOBytes>: TxBytes<'tx> {
  fn from_io(value: T) -> Self;
}

pub trait IntoTx<'tx, T: TxBytes<'tx>>: IOBytes {
  fn into_tx(self) -> T;
}
