use crate::common::id::NodePageId;
use crate::common::layout::node::BranchElement;
use crate::common::layout::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, TxPage};
use crate::pages::Page;
use crate::pages::node::HasNode;
use bytemuck::{Pod, Zeroable};
use delegate::delegate;
use getset::{CopyGetters, Setters};

#[derive(Clone)]
pub struct BranchPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for BranchPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T> BranchPage<T>
where
  T: TxPage<'tx>,
{
  fn elements(&self) -> &[BranchElement] {
    let count = self.page_header().count() as usize;
    let elem_start = size_of::<PageHeader>();
    let elem_end = elem_start + (size_of::<BranchElement>() * count);
    bytemuck::cast_slice(&self.root_page()[elem_start..elem_end])
  }

  fn data_start(&self) -> usize {
    let count = self.page_header().count() as usize;
    let elem_start = size_of::<PageHeader>();
    let elem_end = elem_start + (size_of::<BranchElement>() * count);
    elem_end
  }
}
