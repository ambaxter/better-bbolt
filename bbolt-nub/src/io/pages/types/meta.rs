use crate::common::errors::PageError;
use crate::common::layout::meta::Meta;
use crate::common::layout::page::PageHeader;
use crate::io::TxSlot;
use crate::io::pages::{Page, TxPage, TxPageType};
use bytemuck::from_bytes;
use delegate::delegate;

pub trait HasMeta {
  fn meta(&self) -> &Meta;
}

pub struct MetaPage<'tx, T> {
  page: TxPage<'tx, T>,
}

impl<'tx, T> TryFrom<TxPage<'tx, T>> for MetaPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  type Error = PageError;

  fn try_from(value: TxPage<'tx, T>) -> Result<Self, Self::Error> {
    if value.page.page_header().is_meta() {
      Ok(MetaPage { page: value })
    } else {
      Err(PageError::InvalidMetaFlag(value.page.page_header().flags()))
    }
  }
}

impl<'tx, T> Page for MetaPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  delegate! {
      to &self.page {
      fn root_page(&self) -> &[u8];
      }
  }
}

impl<'tx, T> HasMeta for MetaPage<'tx, T>
where
  T: TxPageType<'tx>,
{
  fn meta(&self) -> &Meta {
    from_bytes(&self.page.root_page()[size_of::<PageHeader>()..size_of::<Meta>()])
  }
}
