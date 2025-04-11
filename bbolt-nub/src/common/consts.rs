use size::{MiB, Size};
use std::time::Duration;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DbTag {
  pub version: u32,
  pub magic: u32,
}

pub const BBOLT_TAG: DbTag = DbTag {
  version: 2,
  magic: 0xED0CDAED,
};

pub const BBOLT_RS_TAG: DbTag = DbTag {
  version: 2,
  // Chosen from https://nedbatchelder.com/text/hexwords.html
  // as we are using the Go BBolt project code as a scaffold
  magic: 0x5caff01d,
};

pub const BETTER_BBOLT_RS_TAG: DbTag = DbTag {
  // Lucky Number 7!
  version: 777,
  // Chosen from https://nedbatchelder.com/text/hexwords.html
  // as we are using the Go BBolt project code as a scaffold
  magic: 0x5caff01d,
};

pub const IGNORE_NO_SYNC: bool = cfg!(target_os = "openbsd");

pub const DEFAULT_MAX_BATCH_SIZE: u32 = 1000;
pub const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(10);
pub const DEFAULT_ALLOC_SIZE: Size = Size::from_const(16 * MiB);
