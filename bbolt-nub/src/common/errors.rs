use crate::common::id::{
  DbPageId, DiskPageId, EOFPageId, FreelistPageId, MetaPageId, NodePageId, OverflowPageId,
};
use crate::common::layout::page::PageFlag;
use crate::io::pages::types::freelist::FreelistPage;
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
  #[error("Expected a node flag. Found `{0:#x}`.")]
  InvalidNodeFlag(PageFlag),
  #[error("Expected a meta flag. Found `{0:#x}`.")]
  InvalidMetaFlag(PageFlag),
  #[error("Expected a freelist flag. Found `{0:#x}`.")]
  InvalidFreelistFlag(PageFlag),
  #[error("Error reading node page `{0:?}`.")]
  InvalidNode(NodePageId),
  #[error("Error reading meta page `{0:?}`.")]
  InvalidMeta(MetaPageId),
  #[error("Error reading freelist page `{0:?}`.")]
  InvalidFreelist(FreelistPageId),
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

#[derive(Debug, Error)]
pub enum OpsError {
  #[error("Ops Error: `TryGet`")]
  TryGet,
  #[error("Ops Error: `TryPartialOrd`")]
  TryPartialOrd,
  #[error("Ops Error: `TryPartialEq`")]
  TryPartialEq,
  #[error("Ops Error: `TryBuf`")]
  TryBuf,
  #[error("Ops Error: `TryHash`")]
  TryHash,
}
