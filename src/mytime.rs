extern crate time;
use self::time::Timespec;

#[derive(Copy,Clone,Serialize,Deserialize)]
pub struct MyTimespec {
  sec: i64,
  nsec: i32,
}

impl MyTimespec {
  pub fn get_time() -> Self{
    Self::from_timespec(&self::time::get_time())
  }

  pub fn from_timespec(timespec: &Timespec) -> Self {
    Self {
      sec: timespec.sec,
      nsec: timespec.nsec,
    }
  }

  pub fn to_timespec(&self) -> time::Timespec {
    time::Timespec::new(self.sec, self.nsec)
  }
}
