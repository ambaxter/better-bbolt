// No noise please while I make stuff up
#![cfg_attr(debug_assertions, allow(dead_code, unused_imports, unused_variables, unused_mut))]

pub use error_stack::Result;

pub mod common;
pub mod io;

pub mod pages;
pub mod tx_io;

pub mod api;
