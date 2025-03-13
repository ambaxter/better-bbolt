use thiserror::Error;
use crate::common::id::DbPageId;
use crate::common::page::PageFlag;

#[derive(Debug, Error)]
pub enum PageError {
  #[error("Expected DbPageId `{0:?}`. Found '{1:?}")]
  UnexpectedDbPageId(DbPageId, DbPageId),
  #[error("Expected PageFlag matching mask `{0:#x}`. Found '{1:#x}")]
  InvalidPageFlag(PageFlag, PageFlag),
}
