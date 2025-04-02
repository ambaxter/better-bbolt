use crate::common::id::NodePageId;
use crate::common::layout::node::{BranchElement, LeafElement};
use crate::common::layout::page::PageHeader;
use crate::io::pages::types::node::leaf::LeafPage;
use crate::io::pages::types::node::{HasKeys, HasNodes};
use crate::io::pages::{GetKvRefSlice, GetKvTxSlice, Page, TxPage, TxPageType};
use bytemuck::{cast_slice, from_bytes};
use delegate::delegate;

pub struct BranchPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
}

impl<'tx, T: 'tx> Page for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T: 'tx> BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn elements(&self) -> &[BranchElement] {
    let elements_len = self.page.page_header().count() as usize;
    let elements_start = size_of::<PageHeader>();
    let elements_end = elements_start + (elements_len * size_of::<BranchElement>());
    cast_slice(&self.page.root_page()[elements_start..elements_end])
  }
}

impl<'tx, T: 'tx> HasKeys<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type RefKv<'a>
    = T::RefKv<'a>
  where
    Self: 'a;
  type TxKv = T::TxKv;

  fn key_ref<'a>(&'a self, index: usize) -> Option<Self::RefKv<'a>> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<BranchElement>());
    let element_end = element_start + size_of::<BranchElement>();
    let element: &BranchElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    let kv_start = element_start + element.key_dist() as usize;
    let key_end = kv_start + element.key_len() as usize;
    Some(self.page.get_ref_slice(kv_start..key_end))
  }

  fn key(&self, index: usize) -> Option<Self::TxKv> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<BranchElement>());
    let element_end = element_start + size_of::<BranchElement>();
    let element: &BranchElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    let kv_start = element_start + element.key_dist() as usize;
    let key_end = kv_start + element.key_len() as usize;
    Some(self.page.get_tx_slice(kv_start..key_end))
  }
}

impl<'tx, T: 'tx> HasNodes<'tx> for BranchPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn node(&self, index: usize) -> Option<NodePageId> {
    let elements_len = self.page.page_header().count() as usize;
    if index > elements_len {
      return None;
    }
    let element_start = size_of::<PageHeader>() + (index * size_of::<BranchElement>());
    let element_end = element_start + size_of::<BranchElement>();
    let element: &BranchElement = from_bytes(&self.page.root_page()[element_start..element_end]);
    Some(element.page_id())
  }
}
