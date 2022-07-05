#![forbid(unsafe_code)]
use std::collections::HashSet;
use std::io::{BufRead, BufWriter};

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
    let handle = stdout.lock();
    let mut buf_writer = BufWriter::new(handle);
    {
        let file = std::fs::File::open(file2).unwrap();
        let mut reader = std::io::BufReader::new(file);
        let mut buffer = String::new();
        if let Ok(_) = reader.read_line(&mut buffer) {
            //for line in reader.lines() {
            // let line = line.unwrap();
            if set.contains(&buffer) {
                buf_writer.write_all(buffer.as_bytes()).unwrap();
                set.remove(&buffer);
            }
            buffer.clear();
        }
    }
}
