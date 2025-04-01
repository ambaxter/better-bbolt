use crate::common::layout::meta::Meta;
use crate::common::layout::page::PageHeader;
use crate::tx_io::TxSlot;
use crate::tx_io::pages::{Page, TxPage, TxPageType};
use bytemuck::from_bytes;
use delegate::delegate;

pub trait HasMeta {
  fn meta(&self) -> &Meta;
}

pub struct MetaPage<'tx, T: 'tx> {
  page: TxPage<'tx, T>,
}

impl<'tx, T: 'tx> Page for MetaPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T: 'tx> HasMeta for MetaPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn meta(&self) -> &Meta {
    from_bytes(&self.page.root_page()[size_of::<PageHeader>()..size_of::<Meta>()])
  }
}
