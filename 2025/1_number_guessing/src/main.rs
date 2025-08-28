use anyhow::Result;
use rand::random_range;
use std::io;

fn read_input() -> Result<u32> {
    let mut inp = String::new();
    io::stdin().read_line(&mut inp)?;
    return Ok(inp.trim().parse()?);
}

fn main() {
    let target: u32 = random_range(1..=100);
    loop {
        println!("Please, enter your number!");
        if let Ok(num) = read_input() {
            if num == target {
                println!("You won!");
                break;
            } else if num < target {
                println!("Too low!");
            } else {
                println!("Too large!");
            }
        }
    }
}
