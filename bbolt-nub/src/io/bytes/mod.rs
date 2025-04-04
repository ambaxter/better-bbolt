use std::ops::Deref;

pub mod ref_bytes;
pub mod shared_bytes;

pub trait IOBytes: Deref<Target = [u8]> + AsRef<[u8]> + Clone + Sized {}

pub trait TxBytes<'tx>: Deref<Target = [u8]> + AsRef<[u8]> + Clone + Sized + Sync + Send {}

pub trait FromIOBytes<'tx, T: IOBytes>: TxBytes<'tx> {
  fn from_io(value: T) -> Self;
}

pub trait IntoTxBytes<'tx, T: TxBytes<'tx>>: IOBytes {
  fn into_tx(self) -> T;
}

impl<'tx, U, T> IntoTxBytes<'tx, U> for T
where
  T: IOBytes,
  U: FromIOBytes<'tx, T>,
{
  fn into_tx(self) -> U {
    U::from_io(self)
  }
}
