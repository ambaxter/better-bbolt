use crate::common::bucket::BucketHeader;
use crate::common::id::{EOFPageId, FreelistPageId, TxId};
use crate::common::layout::meta::Meta;
use crate::common::layout::page::PageHeader;
use crate::io::pages::{HasHeader, HasRootPage, TxPage};
use crate::pages::Page;
use bytemuck::{Pod, Zeroable};
use delegate::delegate;
use fnv_rs::{Fnv64, FnvHasher};
use std::hash::Hasher;

pub trait HasMeta: HasHeader {
  fn meta(&self) -> &Meta;
}

#[derive(Clone)]
pub struct MetaPage<T> {
  page: Page<T>,
}

impl<'tx, T> HasRootPage for MetaPage<T>
where
  T: TxPage<'tx>,
{
  delegate! {
      to &self.page {
          fn root_page(&self) -> &[u8];
      }
  }
}
