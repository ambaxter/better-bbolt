use std::cmp::Ordering;
use std::ops::Range;
use crate::io::pages::SubRange;

pub trait TryIndex {}

pub trait TryPartialEq<Rhs: ?Sized = Self> {
  type Error<'a> where Self: 'a, Rhs: 'a;
  fn try_eq<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>>;
  fn try_ne<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>> {
    self.try_eq(other).map(|ok| !ok)
  }
}

pub trait TryEq: TryPartialEq {}

pub trait TryPartialOrd<Rhs: ?Sized = Self>: TryPartialEq<Rhs> {
  fn try_partial_cmp<'a>(&'a self, other: &'a Rhs) -> Result<Option<Ordering>, Self::Error<'a>>;

  fn lt<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Less)))
  }

  fn le<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Less | Ordering::Equal)))
  }

  fn gt<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Greater)))
  }

  fn ge<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>> {
    self
      .try_partial_cmp(other)
      .map(|ok| matches!(ok, Some(Ordering::Greater | Ordering::Equal)))
  }
}

pub trait IntoTryBuf: Sized {
  type Error;
  type TryBuf: TryBuf<Error = Self::Error>;

  fn into_try_buf(self) -> Result<Self::TryBuf, Self::Error>;
}

impl<Rhs, T> TryPartialEq<Rhs> for T
where
    for<'a> Rhs: 'a,
    for<'a> &'a Rhs: IntoTryBuf<Error = <&'a T as IntoTryBuf>::Error>,
    for<'a> T: 'a,
    for<'a> &'a T: IntoTryBuf,
{
  type Error<'a> = <&'a T as IntoTryBuf>::Error;

  fn try_eq<'a>(&'a self, other: &'a Rhs) -> Result<bool, Self::Error<'a>> {
    let mut s_buf = self.into_try_buf()?;
    let mut o_buf = other.into_try_buf()?;
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
impl<T, Rhs> TryPartialOrd<Rhs> for T
where
    for<'a> Rhs: 'a,
    for<'a> &'a Rhs: IntoTryBuf<Error = <&'a T as IntoTryBuf>::Error>,
    for<'a> T: 'a,
    for<'a> &'a T: IntoTryBuf,
{
  fn try_partial_cmp<'a>(&'a self, other: &'a Rhs) -> Result<Option<Ordering>, Self::Error<'a>> {
    let mut s_buf = self.into_try_buf()?;
    let mut o_buf = other.into_try_buf()?;
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
}

pub trait TryBuf {
  type Error;

  fn remaining(&self) -> usize;

  fn chunk(&self) -> &[u8];

  fn try_advance(&mut self, cnt: usize) -> Result<(), Self::Error>;
}

#[cfg(test)]
mod tests {
  use std::ops::Range;
  use crate::io::ops::{IntoTryBuf, TryBuf, TryPartialEq, TryPartialOrd};
  use crate::io::pages::SubRange;

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
    type Error = &'static str;

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

  impl<'a> IntoTryBuf for &'a ABuf {
    type Error = &'static str;
    type TryBuf = ABufTryBuf<'a>;

    fn into_try_buf(self) -> Result<Self::TryBuf, Self::Error> {
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
    type Error = &'static str;

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

  impl<'a> IntoTryBuf for &'a BBuf {
    type Error = &'static str;
    type TryBuf = BBufTryBuf<'a>;

    fn into_try_buf(self) -> Result<Self::TryBuf, Self::Error> {
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
    let r = TryPartialOrd::ge(&abuf, &bbuf);
    println!("r: {:?}", r);
  }


}
