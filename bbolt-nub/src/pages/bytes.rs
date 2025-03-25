use crate::common::errors::PageError;
use crate::common::id::OverflowPageId;
use crate::common::page::PageHeader;
use crate::io::pages::{HasRootPage, IntoCopiedIterator, SubRange};
use crate::io::{NonContigReader, ReadData};
use crate::pages::{HasHeader, Page};
use delegate::delegate;
use error_stack::ResultExt;
use std::ops::{Deref, Index, Range, RangeBounds};
use triomphe::Arc;
use crate::io::pages::lazy_page::LazyPage;

pub trait TxPageSlice<'tx>:
  Ord + PartialEq<[u8]> + PartialOrd<[u8]> + IntoCopiedIterator<'tx>
{
  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self;
}

// TODO: [u8] wrapper because we have to?
pub trait TxPage<'tx>: AsRef<[u8]> + Clone {
  type TxSlice: TxPageSlice<'tx>;

  fn subslice<R: RangeBounds<usize>>(&self, range: R) -> Self::TxSlice;
}


impl<'tx, RD> HasHeader for LazyPage<RD::PageData, RD>
where
  RD: NonContigReader<'tx>,
{
  delegate! {
      to &self.root {
          fn page_header(&self) -> &PageHeader;
      }
  }
}



