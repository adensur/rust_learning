use std::{
    fmt,
    io::{BufRead, stdin},
};

struct MyAsciiWrapper(Vec<u8>);

impl fmt::Display for MyAsciiWrapper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for &b in &self.0 {
            write!(f, "{}", b as char)?;
        }
        Ok(())
    }
}

impl MyAsciiWrapper {
    fn trim(&self) -> &[u8] {
        let mut start = 0;
        let mut end = self.0.len();
        while start < end && self.0[start].is_ascii_whitespace() {
            start += 1;
        }
        while end > start && self.0[end - 1].is_ascii_whitespace() {
            end -= 1;
        }

        &self.0[start..end]
    }
}

fn main() {
    println!("Please enter your name");
    let mut name = MyAsciiWrapper(Vec::new());
    stdin().lock().read_until(b'\n', &mut name.0).unwrap();
    let name = name.trim();
    let name = std::str::from_utf8(name).unwrap();
    println!("Hello {name}! Howdy today?");
}
