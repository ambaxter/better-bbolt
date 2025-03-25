use crate::io::NonContigReader;
use crate::io::pages::{IntoCopiedIterator, KvDataType, SubRange, SubSlice};
use crate::pages::bytes::{LazyPage, LazySlice, LazySliceIter};
use std::ops::RangeBounds;

impl<'tx, RD> SubSlice<'tx> for LazyPage<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  type Output = LazySlice<RD::PageData, RD>;

  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::Output {
    LazySlice::new(self.clone(), range)
  }
}

impl<'tx, RD> SubSlice<'tx> for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  type Output = LazySlice<RD::PageData, RD>;

  fn sub_slice<R: RangeBounds<usize>>(&self, range: R) -> Self::Output {
    let range = self.range.sub_range(range);
    LazySlice::new(self.page.clone(), range)
  }
}

impl<'tx, RD> IntoCopiedIterator<'tx> for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  type CopiedIter<'a>
    = LazySliceIter<'a, RD::PageData, RD>
  where
    Self: 'a,
    'tx: 'a;

  fn iter_copied<'a>(&'a self) -> Self::CopiedIter<'a>
  where
    'tx: 'a,
  {
    LazySliceIter::new(self)
  }
}

impl<'tx, RD> KvDataType<'tx> for LazySlice<RD::PageData, RD>
where
  RD: NonContigReader<'tx> + 'tx,
{
  fn partial_eq(&self, other: &[u8]) -> bool {
    self.iter_copied().eq(other.iter_copied())
  }

  fn lt(&self, other: &[u8]) -> bool {
    self.iter_copied().lt(other.iter_copied())
  }

  fn le(&self, other: &[u8]) -> bool {
    self.iter_copied().le(other.iter_copied())
  }

  fn gt(&self, other: &[u8]) -> bool {
    self.iter_copied().gt(other.iter_copied())
  }

  fn ge(&self, other: &[u8]) -> bool {
    self.iter_copied().ge(other.iter_copied())
  }

  fn slice_index<R: RangeBounds<usize>>(&self, range: R) -> Self {
    todo!()
  }
}
