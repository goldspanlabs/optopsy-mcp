// float_cmp: only in tests where assert_eq! on f64 is intentional.
#![cfg_attr(test, allow(clippy::float_cmp))]

pub mod data;
pub mod engine;
pub mod server;
pub mod signals;
pub mod strategies;
pub mod tools;
