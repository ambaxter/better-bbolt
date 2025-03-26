use std::marker::PhantomData;

pub mod kv;

pub mod lazy_page;

pub mod ref_page;
pub mod shared_page;


#[derive(Debug, Copy, Clone, Default)]
pub struct TxSlot<'tx> {
  tx: PhantomData<&'tx [u8]>,
}

trait FromTx<T>: Sized {
  fn from_tx<'tx>(value: T) -> Self;
}

trait IntoTx<T>: Sized {
  fn into_tx<'tx>(self) -> T;
}

impl<U, T> IntoTx<T> for U where T: FromTx<U> {
  fn into_tx<'tx>(self) -> T {
    T::from_tx(self)
  }
}