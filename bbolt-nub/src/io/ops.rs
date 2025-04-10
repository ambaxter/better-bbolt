use crate::common::errors::OpsError;
use crate::io::bytes::ref_bytes::{RefBuf, RefTryBuf};
use crate::io::pages::{TxPage, TxPageType};
use error_stack::{FutureExt, ResultExt};
use std::cmp::Ordering;
use std::collections::Bound;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::iter::{Copied, Map};
use std::ops::{Range, RangeBounds};
use std::{io, slice};

use crate::io::pages::lazy::ops::{
  RefIntoTryBuf, RefIntoTryCopiedIter, TryBuf, TryGet, TryHash, TryPartialEq, TryPartialOrd,
};
pub use bytes::Buf;

impl<T> RefIntoTryCopiedIter for T
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn ref_into_try_copied_iter<'a>(
    &'a self,
  ) -> Result<impl Iterator<Item = Result<u8, Self::Error>> + DoubleEndedIterator + 'a, Self::Error>
  {
    Ok(self.as_ref().iter().copied().map(|b| Ok(b)))
  }
}

impl RefIntoTryCopiedIter for [u8] {
  type Error = OpsError;

  fn ref_into_try_copied_iter<'a>(
    &'a self,
  ) -> Result<impl Iterator<Item = Result<u8, Self::Error>> + DoubleEndedIterator + 'a, Self::Error>
  {
    Ok(self.as_ref().iter().copied().map(|b| Ok(b)))
  }
}

pub trait RefIntoCopiedIter {
  type Iter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a;
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a>;
}

impl<T> RefIntoCopiedIter for T
where
  T: AsRef<[u8]>,
{
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;

  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.as_ref().iter().copied()
  }
}

impl RefIntoCopiedIter for [u8] {
  type Iter<'a>
    = Copied<slice::Iter<'a, u8>>
  where
    Self: 'a;
  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    self.iter().copied()
  }
}

impl<T> TryHash for T
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_hash<H: Hasher>(&self, state: &mut H) -> Result<(), Self::Error> {
    Ok(self.as_ref().hash(state))
  }
}

impl TryHash for [u8] {
  type Error = OpsError;
  fn try_hash<H: Hasher>(&self, state: &mut H) -> Result<(), Self::Error> {
    Ok(self.hash(state))
  }
}

impl<T> TryGet<u8> for T
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_get(&self, index: usize) -> crate::Result<Option<u8>, Self::Error> {
    Ok(self.as_ref().get(index).copied())
  }
}

impl TryGet<u8> for [u8] {
  type Error = OpsError;
  fn try_get(&self, index: usize) -> crate::Result<Option<u8>, Self::Error> {
    Ok(self.as_ref().get(index).copied())
  }
}

pub trait RefIntoBuf {
  type Buf<'a>: Buf + 'a
  where
    Self: 'a;

  fn ref_into_buf<'a>(&'a self) -> Self::Buf<'a>;
}

impl<T> RefIntoBuf for T
where
  T: AsRef<[u8]>,
{
  type Buf<'a>
    = RefBuf<'a>
  where
    Self: 'a;

  fn ref_into_buf<'a>(&'a self) -> Self::Buf<'a> {
    RefBuf::new(self.as_ref())
  }
}

impl RefIntoBuf for [u8] {
  type Buf<'a>
    = RefBuf<'a>
  where
    Self: 'a;

  fn ref_into_buf<'a>(&'a self) -> Self::Buf<'a> {
    RefBuf::new(self)
  }
}

impl<T> RefIntoTryBuf for T
where
  T: AsRef<[u8]>,
{
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error> {
    Ok(RefTryBuf::new(self.as_ref()))
  }
}

impl RefIntoTryBuf for [u8] {
  type TryBuf<'a>
    = RefTryBuf<'a>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error> {
    Ok(RefTryBuf::new(self.as_ref()))
  }
}

impl<T, U> TryPartialEq<U> for T
where
  T: AsRef<[u8]>,
  U: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &U) -> crate::Result<bool, Self::Error> {
    Ok(self.as_ref().eq(other.as_ref()))
  }
}

impl<T> TryPartialEq<[u8]> for T
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &[u8]) -> crate::Result<bool, Self::Error> {
    Ok(self.as_ref().eq(other))
  }
}

impl<T> TryPartialEq<T> for [u8]
where
  T: AsRef<[u8]>,
{
  type Error = OpsError;

  fn try_eq(&self, other: &T) -> crate::Result<bool, Self::Error> {
    Ok(self.eq(other.as_ref()))
  }
}

impl<T, U> TryPartialOrd<U> for T
where
  T: AsRef<[u8]>,
  U: AsRef<[u8]>,
{
  fn try_partial_cmp<'a>(&'a self, other: &'a U) -> crate::Result<Option<Ordering>, Self::Error> {
    Ok(self.as_ref().partial_cmp(other.as_ref()))
  }
}

impl<T> TryPartialOrd<[u8]> for T
where
  T: AsRef<[u8]>,
{
  fn try_partial_cmp<'a>(
    &'a self, other: &'a [u8],
  ) -> crate::Result<Option<Ordering>, Self::Error> {
    Ok(self.as_ref().partial_cmp(other))
  }
}

impl<T> TryPartialOrd<T> for [u8]
where
  T: AsRef<[u8]>,
{
  fn try_partial_cmp<'a>(&'a self, other: &'a T) -> crate::Result<Option<Ordering>, Self::Error> {
    Ok(self.partial_cmp(other.as_ref()))
  }
}

/*
#[cfg(test)]
mod tests {
  use crate::common::errors::OpsError;
  use crate::io::ops::{RefIntoTryBuf, SubRange};
  use crate::io::ops::{TryBuf, TryPartialEq, TryPartialOrd};
  use std::io;
  use std::ops::Range;

  pub struct ABuf {
    bytes: Vec<u8>,
    max_chunk_len: usize,
  }

  pub struct ABufTryBuf<'a> {
    bytes: &'a [u8],
    range: Range<usize>,
    max_chunk_len: usize,
  }

  impl<'a> TryBuf for ABufTryBuf<'a> {
    type Error = OpsError;

    fn remaining(&self) -> usize {
      self.range.len()
    }

    fn chunk(&self) -> &[u8] {
      let len = self.range.len().min(self.max_chunk_len);
      let range = self.range.sub_range(0..len);
      &self.bytes[range]
    }

    fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
      self.range = self.range.sub_range(cnt..);
      Ok(())
    }
  }

  impl RefIntoTryBuf for ABuf {
    type TryBuf<'a> = ABufTryBuf<'a>;

    fn ref_into_try_buf<'a>(
      &'a self,
    ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error>
    {
      Ok(ABufTryBuf {
        bytes: &self.bytes,
        range: 0..self.bytes.len(),
        max_chunk_len: self.max_chunk_len,
      })
    }
  }

  pub struct BBuf {
    bytes: Vec<u8>,
    max_chunk_len: usize,
  }

  pub struct BBufTryBuf<'a> {
    bytes: &'a [u8],
    range: Range<usize>,
    max_chunk_len: usize,
  }

  impl<'a> TryBuf for BBufTryBuf<'a> {
    type Error = OpsError;

    fn remaining(&self) -> usize {
      self.range.len()
    }

    fn chunk(&self) -> &[u8] {
      let len = self.range.len().min(self.max_chunk_len);
      let range = self.range.sub_range(0..len);
      &self.bytes[range]
    }

    fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
      self.range = self.range.sub_range(cnt..);
      Ok(())
    }
  }

  impl RefIntoTryBuf for BBuf {
    type TryBuf<'a> = BBufTryBuf<'a>;

    fn ref_into_try_buf<'a>(
      &'a self,
    ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error>
    {
      Ok(BBufTryBuf {
        bytes: &self.bytes,
        range: 0..self.bytes.len(),
        max_chunk_len: self.max_chunk_len,
      })
    }
  }

  #[test]
  fn eq_test() {
    let data = vec![1, 2, 3, 4, 5];
    let abuf = ABuf {
      bytes: data.clone(),
      max_chunk_len: 2,
    };
    let bbuf = BBuf {
      bytes: data.clone(),
      max_chunk_len: 3,
    };
    let r = TryPartialEq::try_ne(&abuf, &bbuf);
    println!("r: {:?}", r);
  }

  #[test]
  fn ord_test() {
    let abuf = ABuf {
      bytes: vec![1, 2, 3, 4, 5],
      max_chunk_len: 2,
    };
    let bbuf = BBuf {
      bytes: vec![1, 2, 3, 4, 5],
      max_chunk_len: 4,
    };
    let r = TryPartialOrd::try_ge(&abuf, &bbuf);
    println!("r: {:?}", r);
  }

  #[test]
  fn ord_slice_test() {
    let r = vec![1, 2, 3, 4, 5];
    let bbuf = BBuf {
      bytes: vec![1, 2, 3, 4, 5],
      max_chunk_len: 4,
    };
    let r = TryPartialOrd::try_ge(&r, &bbuf);
    println!("r: {:?}", r);
  }
}
*/
