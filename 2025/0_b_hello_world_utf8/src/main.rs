use std::io;

fn my_trim(s: &str) -> &str {
    let mut start = 0;
    for (idx, c) in s.char_indices() {
        start = idx;
        if !c.is_whitespace() {
            break;
        }
    }
    let mut end = s.len();
    for (idx, c) in s.char_indices().rev() {
        end = idx;
        if !c.is_whitespace() {
            break;
        }
    }
    &s[start..=end]
}

fn main() {
    println!("Enter your name!");
    let mut name = String::new();
    io::stdin().read_line(&mut name).unwrap();
    let name = my_trim(&name);
    println!("Hello {name}!");
}
