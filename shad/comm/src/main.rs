#![forbid(unsafe_code)]
use std::collections::HashSet;
use std::io::BufRead;

use std::io::{self, Write};

fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    let file1 = &args[1];
    let file2 = &args[2];
    let mut set: HashSet<String> = HashSet::new();
    {
        let file = std::fs::File::open(file1).unwrap();
        let reader = std::io::BufReader::new(file);
        for line in reader.lines() {
            set.insert(line.unwrap());
        }
    }
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    {
        let file = std::fs::File::open(file2).unwrap();
        let reader = std::io::BufReader::new(file);
        for line in reader.lines() {
            let line = line.unwrap();
            if set.contains(&line) {
                handle.write_all(line.as_bytes()).unwrap();
                handle.write("\n".as_bytes()).unwrap();
                set.remove(&line);
            }
        }
    }
}
