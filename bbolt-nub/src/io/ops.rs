use crate::common::errors::OpsError;
use crate::io::pages::{TxPage, TxPageType};
use std::cmp::Ordering;
use std::collections::Bound;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::io;
use std::ops::{Range, RangeBounds};

pub trait SubRange {
  fn sub_range<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

impl SubRange for Range<usize> {
  fn sub_range<R: RangeBounds<usize>>(&self, range: R) -> Self {
    let start = match range.start_bound().cloned() {
      Bound::Included(start) => self.start + start,
      Bound::Excluded(start) => self.start + start + 1,
      Bound::Unbounded => self.start,
    };
    let end = match range.end_bound().cloned() {
      Bound::Included(end) => self.start + end + 1,
      Bound::Excluded(end) => self.start + end,
      Bound::Unbounded => self.end,
    };
    assert!(
      start <= end,
      "New start ({start}) should be <= new end ({end})"
    );
    assert!(
      end <= self.end,
      "New end ({end}) should be <= current end ({0})",
      self.end
    );
    start..end
  }
}

pub trait RefIntoCopiedIter {
  type Iter<'a>: Iterator<Item = u8> + DoubleEndedIterator + ExactSizeIterator + 'a
  where
    Self: 'a;
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a>;
}

pub trait TryHash {
  type Error: Error + Send + Sync;

  fn try_hash<H: Hasher>(&self, state: &mut H) -> Result<(), Self::Error>;
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

pub trait TryGet<T> {
  type Error: Error + Send + Sync;

  fn try_get(&self, index: usize) -> crate::Result<Option<T>, Self::Error>;
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

pub trait TryPartialEq<Rhs: ?Sized = Self> {
  type Error: Error + Send + Sync;
  fn try_eq<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error>;
  fn try_ne<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error> {
    self.try_eq(other).map(|ok| !ok)
  }
}

pub trait TryEq: TryPartialEq {}

pub trait TryPartialOrd<Rhs: ?Sized = Self>: TryPartialEq<Rhs> {
  fn try_partial_cmp<'a>(&'a self, other: &'a Rhs) -> crate::Result<Option<Ordering>, Self::Error>;

  fn try_lt<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Less)))
  }

  fn try_le<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Less | Ordering::Equal)))
  }

  fn try_gt<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Greater)))
  }

  fn try_ge<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Greater | Ordering::Equal)))
  }
}

pub trait RefIntoTryBuf {
  type Error: Error + Send + Sync;
  type TryBuf<'a>: TryBuf<Error = Self::Error>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(&'a self) -> crate::Result<Self::TryBuf<'a>, Self::Error>;
}
/*
impl<Rhs: ?Sized, T: ?Sized> TryPartialEq<Rhs> for T
{
  type Error = <T as RefIntoTryBuf>::Error;

  fn try_eq<'a>(&'a self, other: &'a Rhs) -> crate::Result<bool, Self::Error> {
    let mut s_buf = self.ref_into_try_buf()?;
    let mut o_buf = other.ref_into_try_buf()?;
    if s_buf.remaining() != o_buf.remaining() {
      return Ok(false);
    }
    while s_buf.remaining() > 0 {
      let s_chunk = s_buf.chunk();
      let o_chunk = o_buf.chunk();
      let cmp_len = s_chunk.len().min(o_chunk.len());
      //TODO: What do we do here?
      assert_ne!(0, cmp_len);
      let s_cmp = &s_chunk[..cmp_len];
      let o_cmp = &o_chunk[..cmp_len];
      if s_cmp != o_cmp {
        return Ok(false);
      }
      s_buf.try_advance(cmp_len)?;
      o_buf.try_advance(cmp_len)?;
    }
    Ok(true)
  }
}

// RustRover doesn't like this
impl<T: ?Sized, Rhs: ?Sized> TryPartialOrd<Rhs> for T
where
  Rhs: RefIntoTryBuf<Error = <T as RefIntoTryBuf>::Error>,
  T: RefIntoTryBuf,
{
  fn try_partial_cmp<'a>(&'a self, other: &'a Rhs) -> crate::Result<Option<Ordering>, Self::Error> {
    let mut s_buf = self.ref_into_try_buf()?;
    let mut o_buf = other.ref_into_try_buf()?;
    while s_buf.remaining() > 0 && o_buf.remaining() > 0 {
      let s_chunk = s_buf.chunk();
      let o_chunk = o_buf.chunk();
      let cmp_len = s_chunk.len().min(o_chunk.len());
      assert_ne!(0, cmp_len);
      let s_cmp = &s_chunk[..cmp_len];
      let o_cmp = &o_chunk[..cmp_len];
      let cmp = s_cmp.cmp(o_cmp);
      if cmp != Ordering::Equal {
        return Ok(Some(cmp));
      }
      s_buf.try_advance(cmp_len)?;
      o_buf.try_advance(cmp_len)?;
    }
    Ok(s_buf.remaining().partial_cmp(&o_buf.remaining()))
  }
}*/

pub trait TryBuf {
  type Error: Error + Send + Sync;

  fn remaining(&self) -> usize;

  fn chunk(&self) -> &[u8];

  fn try_advance(&mut self, cnt: usize) -> Result<(), Self::Error>;
}

pub trait KvEq: Eq + PartialEq<[u8]> /* + TryPartialEq + TryPartialEq<[u8]> */ +  Sized {}

pub trait KvOrd: Ord + PartialOrd<[u8]> /*+ TryPartialOrd + TryPartialOrd<[u8]> */ + KvEq {}

pub trait KvDataType:
  KvOrd + TryHash + Hash + TryGet<u8> + RefIntoCopiedIter + RefIntoTryBuf
{
}

pub trait GetKvRefSlice {
  type RefKv<'a>: GetKvRefSlice + KvDataType + 'a
  where
    Self: 'a;
  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a>;
}

pub trait GetKvTxSlice<'tx>: GetKvRefSlice {
  type TxKv: GetKvTxSlice<'tx> + KvDataType + 'tx;
  fn get_tx_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxKv;
}

#[cfg(test)]
mod tests {
  use crate::io::ops::SubRange;
  use crate::io::ops::{RefIntoTryBuf, TryBuf, TryPartialEq, TryPartialOrd};
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
    type Error = io::Error;

    fn remaining(&self) -> usize {
      self.range.len()
    }

    fn chunk(&self) -> &[u8] {
      let len = self.range.len().min(self.max_chunk_len);
      let range = self.range.sub_range(0..len);
      &self.bytes[range]
    }

    fn try_advance(&mut self, cnt: usize) -> Result<(), Self::Error> {
      self.range = self.range.sub_range(cnt..);
      Ok(())
    }
  }

  impl RefIntoTryBuf for ABuf {
    type Error = io::Error;
    type TryBuf<'a> = ABufTryBuf<'a>;

    fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
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
    type Error = io::Error;

    fn remaining(&self) -> usize {
      self.range.len()
    }

    fn chunk(&self) -> &[u8] {
      let len = self.range.len().min(self.max_chunk_len);
      let range = self.range.sub_range(0..len);
      &self.bytes[range]
    }

    fn try_advance(&mut self, cnt: usize) -> Result<(), Self::Error> {
      self.range = self.range.sub_range(cnt..);
      Ok(())
    }
  }

  impl RefIntoTryBuf for BBuf {
    type Error = io::Error;
    type TryBuf<'a> = BBufTryBuf<'a>;

    fn ref_into_try_buf<'a>(&'a self) -> Result<Self::TryBuf<'a>, Self::Error> {
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
    let r = TryPartialOrd::try_ge(r.as_slice(), &bbuf);
    println!("r: {:?}", r);
  }
}
