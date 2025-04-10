use crate::common::errors::{OpsError, PageError};
use crate::io::ops::{
  GetKvRefSlice, KvDataType, KvEq, KvOrd, RefIntoCopiedIter, RefIntoTryBuf, SubRange, TryBuf,
  TryGet, TryHash, TryPartialEq, TryPartialOrd,
};
use crate::io::pages::lazy::{LazyIter, LazyPage};
use crate::io::pages::{TxPageType, TxReadLazyPageIO, TxReadPageIO};
use std::cmp::Ordering;
use std::hash;
use std::ops::{Range, RangeBounds};

pub struct LazyRefSlice<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> {
  page: &'a LazyPage<'tx, L>,
  range: Range<usize>,
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> LazyRefSlice<'a, 'tx, L> {
  pub fn new(page: &'a LazyPage<'tx, L>, range: Range<usize>) -> Self {
    Self { page, range }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Clone for LazyRefSlice<'a, 'tx, L> {
  fn clone(&self) -> Self {
    LazyRefSlice {
      page: self.page,
      range: self.range.clone(),
    }
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialEq for LazyRefSlice<'a, 'tx, L> {
  fn eq(&self, other: &Self) -> bool {
    TryPartialEq::try_eq(self, other).expect("partial_eq error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialEq<[u8]> for LazyRefSlice<'a, 'tx, L> {
  fn eq(&self, other: &[u8]) -> bool {
    TryPartialEq::try_eq(self, &other).expect("partial_eq error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Eq for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialOrd for LazyRefSlice<'a, 'tx, L> {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    TryPartialOrd::try_partial_cmp(self, other).expect("partial_ord error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> PartialOrd<[u8]> for LazyRefSlice<'a, 'tx, L> {
  fn partial_cmp(&self, other: &[u8]) -> Option<Ordering> {
    TryPartialOrd::try_partial_cmp(self, &other).expect("partial_ord error")
  }
}

impl<'a, 'tx: 'a, L: TxReadLazyPageIO<'tx>> Ord for LazyRefSlice<'a, 'tx, L> {
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .ref_into_copied_iter()
      .cmp(other.ref_into_copied_iter())
  }
}

impl<'p, 'tx: 'p, L: TxReadLazyPageIO<'tx>> GetKvRefSlice for LazyRefSlice<'p, 'tx, L> {
  type RefKv<'a>
    = LazyRefSlice<'a, 'tx, L>
  where
    Self: 'a,
    'p: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    LazyRefSlice {
      page: self.page,
      range: self.range.sub_range(range),
    }
  }
}

impl<'p, 'tx: 'p, L: TxReadLazyPageIO<'tx>> RefIntoCopiedIter for LazyRefSlice<'p, 'tx, L> {
  type Iter<'a>
    = LazyIter<'a, 'tx, L>
  where
    Self: 'a;

  #[inline]
  fn ref_into_copied_iter<'a>(&'a self) -> Self::Iter<'a> {
    LazyIter::new(self.page, self.range.clone())
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> TryHash for LazyRefSlice<'p, 'tx, L> {
  type Error = OpsError;

  fn try_hash<H: hash::Hasher>(&self, state: &mut H) -> Result<(), Self::Error> {
    todo!()
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> hash::Hash for LazyRefSlice<'p, 'tx, L> {
  fn hash<H: hash::Hasher>(&self, state: &mut H) {
    self.try_hash(state).expect("hashing error")
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> TryGet<u8> for LazyRefSlice<'p, 'tx, L> {
  type Error = OpsError;

  fn try_get(&self, index: usize) -> error_stack::Result<Option<u8>, Self::Error> {
    todo!()
  }
}

impl<'p, 'tx, L: TxReadLazyPageIO<'tx>> RefIntoTryBuf for LazyRefSlice<'p, 'tx, L> {
  type TryBuf<'a>
    = LazyRefTryBuf<'a, 'tx, L>
  where
    Self: 'a;

  fn ref_into_try_buf<'a>(
    &'a self,
  ) -> crate::Result<Self::TryBuf<'a>, <<Self as RefIntoTryBuf>::TryBuf<'a> as TryBuf>::Error> {
    todo!()
  }
}


pub struct LazyRefTryBuf<'a, 'tx, L: TxReadLazyPageIO<'tx>> {
  slice: LazyRefSlice<'a, 'tx, L>,
  range: Range<usize>,
  page: <<L as TxReadPageIO<'tx>>::TxPageType as TxPageType<'tx>>::TxPageBytes,
  overflow_index: u32,
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> LazyRefTryBuf<'a, 'tx, L> {
  pub fn new(slice: &LazyRefSlice<'a, 'tx, L>) -> crate::Result<Self, PageError> {
    let range = slice.range.clone();
    let overflow_index = (range.start / slice.page.root.as_ref().len()) as u32;
    let page_result = if overflow_index == 0 {
      Ok(slice.page.root.clone())
    } else {
      slice.page.read_overflow_page(overflow_index)
    };
    page_result.map(|page| LazyRefTryBuf {
      slice: (*slice).clone(),
      range,
      page,
      overflow_index,
    })
  }
}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> TryBuf for LazyRefTryBuf<'a, 'tx, L> {
  type Error = PageError;

  fn remaining(&self) -> usize {
    self.range.len()
  }

  fn chunk(&self) -> &[u8] {
    let page_len = self.page.as_ref().len();
    todo!()
  }

  fn try_advance(&mut self, cnt: usize) -> crate::Result<(), Self::Error> {
    todo!()
  }
}


impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> KvEq for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> KvOrd for LazyRefSlice<'a, 'tx, L> {}

impl<'a, 'tx, L: TxReadLazyPageIO<'tx>> KvDataType for LazyRefSlice<'a, 'tx, L> {}