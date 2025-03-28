use std::ops::{Range, RangeBounds};
use triomphe::Arc;
use crate::tx_io::TxSlot;
use crate::tx_io::backends::ReadLazyIO;
use crate::tx_io::bytes::TxBytes;
use crate::tx_io::pages::{GetKvRefSlice, GetKvTxSlice, Page, ReadLazyPageIO, RefIntoCopiedIter, SubRange};

#[derive(Clone)]
pub struct LazyPage<'tx, L: ReadLazyPageIO<'tx>> {
  tx: TxSlot<'tx>,
  root: L::PageBytes,
  r: Option<&'tx L>,
}

impl<'tx, L: ReadLazyPageIO<'tx>> Page<'tx> for LazyPage<'tx, L> {
  fn root_page(&self) -> &[u8] {
    self.root.as_ref()
  }
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> IntoIterator for &'a LazyPage<'tx, L> {
  type Item = u8;
  type IntoIter = LazyIter<'a, 'tx,  L>;

  fn into_iter(self) -> Self::IntoIter {
    LazyIter {
      page: self,
      range: 0..self.root_page().len(),
    }
  }
}

#[derive(Clone)]
pub struct LazyIter<'a, 'tx: 'a,  L: ReadLazyPageIO<'tx>> {
  page: &'a LazyPage<'tx, L>,
  range: Range<usize>,
}

impl<'a, 'tx: 'a, L: ReadLazyPageIO<'tx>> LazyIter<'a, 'tx,  L> {

}

impl<'a, 'tx:'a , L: ReadLazyPageIO<'tx>> Iterator for LazyIter<'a,'tx, L> {
  type Item = u8;
  fn next(&mut self) -> Option<Self::Item> {
    todo!()
  }
}

#[derive(Clone)]
pub struct LazyRefSlice<'a, L:ReadLazyPageIO<'a>> {
  page: &'a LazyPage<'a, L>,
  range: Range<usize>,
}

impl<'p, L:ReadLazyPageIO<'p>> GetKvRefSlice for LazyRefSlice<'p, L> {
  type RefKv<'a> = LazyRefSlice<'p, L>
  where
    Self: 'a, 'p: 'a;

  fn get_ref_slice<'a, R: RangeBounds<usize>>(&'a self, range: R) -> Self::RefKv<'a> {
    LazyRefSlice {
      page: self.page,
      range: self.range.sub_range(range),
    }
  }
}

#[derive(Clone)]
pub struct LazyTxSlice<'tx, L: ReadLazyPageIO<'tx>> {
  page: LazyPage<'tx, L>,
  range: Range<usize>,
}
