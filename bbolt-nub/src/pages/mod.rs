use crate::common::errors::PageError;
use crate::common::id::DbPageType;
use crate::common::layout::page::PageHeader;
use crate::io::ReadData;
use crate::io::pages::{HasRootPage, Page, TxPage};
use std::ops::{Deref, RangeBounds};

pub mod freelist;
pub mod meta;
pub mod node;

/*
impl<'tx, T> Page<T>
where
  T: TxPage<'tx>,
{
  pub fn new<D: DbPageType>(page_id: D, buffer: T) -> error_stack::Result<Self, PageError> {
    let p = Page { buffer };
    p.page_header().fast_check(page_id).map(|_| p)
  }
}
*/
