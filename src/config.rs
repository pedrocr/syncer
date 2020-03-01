extern crate toml;
extern crate rand;
extern crate hex;

use self::rand::RngCore;
use self::rand::os::OsRng;

use std::path::Path;
use std::fs::File;
use std::io::{Read,Write};

use crate::settings::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
  #[serde(default)]
  pub formatversion: u64,
  pub server: String,
  pub maxbytes: u64,
  #[serde(default)]
  pub peerid: String,
}

pub fn convert_peerid(peerid: &str) -> i64 {
  let vals = hex::decode(peerid).unwrap();
  let mut val: u64 = 0;
  for v in vals {
    val <<= 8;
    val |= v as u64;
  }
  val as i64
}

impl Config {
  pub fn new(server: String, maxbytes: u64) -> Self {
    let mut rng = OsRng::new().unwrap();
    let mut bytes = [0u8; 8];
    rng.fill_bytes(&mut bytes);

    Self {
      formatversion: FORMATVERSION,
      server,
      maxbytes,
      peerid: hex::encode(&bytes),
    }
  }

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
    let config: Config = match toml::from_str(&conf) {
      Ok(c) => c,
      Err(e) => return Err(format!("couldn't parse config file: {}", e)),
    };

    if config.peerid.len() != 16 {
      return Err(format!("invalid peer: {:?}", config.peerid));
    }
    if !hex::decode(&config.peerid).is_ok() {
      return Err(format!("invalid peer: {:?}", config.peerid));
    }
    Ok(config)
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

  pub fn peernum(&self) -> i64 {
    convert_peerid(&self.peerid)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn peernum_roundtrips() {
    let vals = [1,1,1,1];
    let text = hex::encode(&vals);
    assert_eq!(16843009, convert_peerid(&text));
  }
}
