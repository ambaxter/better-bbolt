use crate::io::pages::GetGatKvRefSlice;
use std::cmp::Ordering;
use std::error::Error;
use std::hash::Hasher;

pub trait RefIntoTryCopiedIter {
  type Error: Error + Send + Sync + 'static;

  // TODO: Impl trait is not allowed for associated types. Fix this when possible
  fn ref_into_try_copied_iter<'a>(
    &'a self,
  ) -> crate::Result<
    impl Iterator<Item = crate::Result<u8, Self::Error>> + DoubleEndedIterator + 'a,
    Self::Error,
  >;
}

pub trait TryGet<T> {
  type Error: Error + Send + Sync + 'static;

  fn try_get(&self, index: usize) -> crate::Result<Option<T>, Self::Error>;
}

pub trait TryHash {
  type Error: Error + Send + Sync + 'static;

  fn try_hash<H: Hasher>(&self, state: &mut H) -> Result<(), Self::Error>;
}

pub trait TryBuf: Sized {
  type Error: Error + Send + Sync + 'static;

  fn remaining(&self) -> usize;

  fn chunk(&self) -> &[u8];

  fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error>;
}

pub trait RefIntoTryBuf {
  type TryBuf<'a>: TryBuf + 'a
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error>;
}

pub trait LazyRefIntoTryBuf: RefIntoTryBuf {}

pub trait TryPartialEq<Rhs: ?Sized = Self> {
  type Error: Error + Send + Sync + 'static;
  fn try_eq(&self, other: &Rhs) -> crate::Result<bool, Self::Error>;
  fn try_ne(&self, other: &Rhs) -> crate::Result<bool, Self::Error> {
    self.try_eq(other).map(|ok| !ok)
  }
}

pub trait TryEq: TryPartialEq<Self> {}

//TODO: TryOrd
pub trait TryPartialOrd<Rhs: ?Sized = Self>: TryPartialEq<Rhs> {
  fn try_partial_cmp(&self, other: &Rhs) -> crate::Result<Option<Ordering>, Self::Error>;

  fn try_lt(&self, other: &Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Less)))
  }

  fn try_le(&self, other: &Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Less | Ordering::Equal)))
  }

  fn try_gt<'a>(&self, other: &Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Greater)))
  }

  fn try_ge(&self, other: &Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Greater | Ordering::Equal)))
  }
}

pub trait KvTryEq: TryEq + TryPartialEq<[u8]> {}

pub trait KvTryOrd: TryPartialOrd + TryPartialOrd<[u8]> + KvTryEq {}

pub trait KvTryDataType:
  KvTryOrd + TryHash + TryGet<u8> + RefIntoTryCopiedIter + RefIntoTryBuf + GetGatKvRefSlice + Sized
{
}
