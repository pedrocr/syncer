#[macro_use] extern crate lazy_static;

mod filesystem;
mod blockstorage;

pub use filesystem::run;
