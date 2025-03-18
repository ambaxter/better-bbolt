use crate::common::id::{DbPageId, DiskPageId, EOFPageId};
use crate::common::page::PageFlag;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageError {
  #[error("Expected DbPageId `{0:?}`. Found '{1:?}")]
  UnexpectedDbPageId(DbPageId, DbPageId),
  #[error("Expected PageFlag matching mask `{0:#x}`. Found '{1:#x}")]
  InvalidPageFlag(PageFlag, PageFlag),
  #[error("Page overflow unsupported for {0:?}. Flags '{1:#x}")]
  UnsupportedPageFlagOverflow(DbPageId, PageFlag),
}

#[derive(Debug, Error)]
pub enum DiskReadError {
  #[error("UnexpectedEOF: Read to `{0:?}`. EOF at '{1:?}.")]
  UnexpectedEOF(DiskPageId, EOFPageId),
  #[error("ReadError: Read at `{0:?}`.")]
  ReadError(DiskPageId),
}
