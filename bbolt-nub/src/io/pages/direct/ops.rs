use crate::io::ops::{RefIntoBuf, RefIntoCopiedIter};
use std::hash::Hash;
use std::ops::RangeBounds;

pub trait Get<T> {
  fn get<'a>(&'a self, index: usize) -> Option<T>;
}

pub trait KvEq: Eq + PartialEq<[u8]> {}

pub trait KvOrd: Ord + PartialOrd<[u8]> + KvEq {}

pub trait KvDataType: KvOrd + Hash + Get<u8> + RefIntoCopiedIter + RefIntoBuf + Sized {}
