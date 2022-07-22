use anyhow::{Context, Result};
use std::{error::Error, fmt};
use thiserror::Error;
struct MyError {}

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Display for MyError!")
    }
}

impl fmt::Debug for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Debug for MyError!")
    }
}

impl std::error::Error for MyError {}

fn my_func() -> Result<(), MyError> {
    Err(MyError {})
}

#[derive(Error, Debug)]
enum MyError2 {
    #[error("the data for key `{0}` is not available")]
    Redaction(String),
}

fn my_func2() -> Result<(), MyError2> {
    Err(MyError2::Redaction("My Error #2 works!".into()))
}

fn main() -> Result<()> {
    println!("Hello, world!");
    my_func2()?;
    my_func()?;
    Ok(())
}
