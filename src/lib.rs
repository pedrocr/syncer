#[macro_use] extern crate lazy_static;

mod filesystem;
mod blobstorage;

pub use filesystem::run;
