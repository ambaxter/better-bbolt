use crate::common::id::{DbPageId, DiskPageId, EOFPageId, OverflowPageId};
use crate::common::layout::page::PageFlag;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PageError {
  #[error("Expected DbPageId `{0:?}`. Found '{1:?}")]
  UnexpectedDbPageId(DbPageId, DbPageId),
  #[error("Expected PageFlag matching mask `{0:#x}`. Found '{1:#x}")]
  InvalidPageFlag(PageFlag, PageFlag),
  #[error("Error while loading page {0:?} overflow {1} ")]
  OverflowReadError(OverflowPageId, u32),
}

#[derive(Debug, Error)]
pub enum DiskReadError {
  #[error("ReadError: Unable to open file at `{0:?}`.")]
  OpenError(PathBuf),
  #[error("ReadError: Unable to understand file metadata.")]
  MetaError,
  #[error("UnexpectedEOF: Read to `{0:?}`. EOF at '{1:?}.")]
  UnexpectedEOF(DiskPageId, EOFPageId),
  #[error("ReadError: Read at `{0:?}`.")]
  ReadError(DiskPageId),
}
