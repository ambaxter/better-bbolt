use crate::io::ops::RefIntoBuf;
use std::hash::Hash;
use std::ops::RangeBounds;

pub trait DirectGet<T> {
  fn direct_get(&self, index: usize) -> Option<T>;
}

pub trait KvEq: Eq + PartialEq<[u8]> {}

pub trait KvOrd: Ord + PartialOrd<[u8]> + KvEq {}

pub trait KvDataType: KvOrd + Hash + DirectGet<u8> + RefIntoBuf {}
