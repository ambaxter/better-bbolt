use crate::common::layout::node::LeafElement;
use crate::common::layout::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, TxPage};
use crate::pages::Page;
use crate::pages::node::branch::BranchPage;
use bytemuck::{Pod, Zeroable};
use delegate::delegate;
use getset::{CopyGetters, Setters};

#[derive(Clone)]
pub struct LeafPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for LeafPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T> LeafPage<T>
where
  T: TxPage<'tx>,
{
  fn elements(&self) -> &[LeafElement] {
    let count = self.page_header().count() as usize;
    let elem_start = size_of::<PageHeader>();
    let elem_end = elem_start + (size_of::<LeafElement>() * count);
    bytemuck::cast_slice(&self.root_page()[elem_start..elem_end])
  }
}
