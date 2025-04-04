#![allow(warnings)]

pub fn add(left: u64, right: u64) -> u64 {
  left + right
}
// The initial port was a fever pitched battle between myself, my ADHD, and understanding Go code
// After much reflecting I know I can do it better
pub mod backend;
pub mod pages;

pub mod common;

pub mod cursor;
pub mod index;

pub mod io;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn it_works() {
    let result = add(2, 2);
    assert_eq!(result, 4);
  }
}
