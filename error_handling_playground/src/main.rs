use std::{error::Error, fmt};

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

fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");
    my_func()?;
    Ok(())
}
