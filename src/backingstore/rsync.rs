use std::process::Command;
use std::io::{Error, ErrorKind};
use std::ffi::{OsString, OsStr};

pub struct RsyncCommand {
  args: Vec<OsString>,
}

impl RsyncCommand {
  pub fn new() -> Self {
    Self {
      args: Vec::new(),
    }
  }

  pub fn arg<S: AsRef<OsStr>>(&mut self, s: S) -> &mut Self {
    self.args.push(s.as_ref().to_os_string());
    self
  }

  pub fn run(&self) -> Result<(), Error> {
    for _ in 0..10 {
      let mut cmd = Command::new("rsync");
      cmd.arg("--quiet");
      cmd.arg("--timeout=5");
      // --whole-file is needed instead of --append because otherwise concurrent usage while
      // doing readhead causes short blocks
      cmd.arg("--whole-file");
      cmd.args(&self.args);
      match cmd.status() {
        Ok(v) => {
          if v.success() {
            return Ok(())
          } else {
            continue
          }
        },
        Err(_) => {},
      }
    }
    Err(Error::new(ErrorKind::Other, "rsync failed"))
  }
}
