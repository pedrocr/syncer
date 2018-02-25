extern crate toml;

use std::path::Path;
use std::fs::File;
use std::io::{Read,Write};

#[derive(Serialize, Deserialize)]
pub struct Config {
  pub server: String,
  pub maxbytes: u64,
}

impl Config {
  pub fn fetch_config(path: &Path) -> Result<Config, String> {
    let mut file = match File::open(path) {
      Ok(f) => f,
      Err(e) => return Err(format!("couldn't open config file: {}", e)),
    };
    let mut conf = String::new();
    match file.read_to_string(&mut conf) {
      Ok(f) => f,
      Err(e) => return Err(format!("couldn't read config file: {}", e)),
    };
    match toml::from_str(&conf) {
      Ok(c) => Ok(c),
      Err(e) => Err(format!("couldn't parse config file: {}", e)),
    }
  }

  pub fn save_config(&self, path: &Path) -> Result<(), String> {
    let serial = match toml::to_string(self) {
      Ok(c) => c,
      Err(e) => return Err(format!("couldn't write config file: {}", e)),
    };
    let mut file = match File::create(path) {
      Ok(f) => f,
      Err(e) => return Err(format!("couldn't open config file: {}", e)),
    };
    match file.write_all(&serial.into_bytes()) {
      Ok(_) => {},
      Err(e) => return Err(format!("couldn't write config file: {}", e)),
    };
    Ok(())
  }
}
