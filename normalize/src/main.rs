use ascii::AsciiString;

fn normalize(str: &mut AsciiString) {
    let mut w = 0;
    let mut is_space = false;
    for i in 0..str.len() {
        if str[i] == b' ' {
            if is_space {
                continue;
            }
            is_space = true;
        }
        str[w] = str[i];
        w += 1;
    }
    str.truncate(w);
    // this next one doesn't work - iterator functions are immutable borrows, need mutable borrow as well for inplace modification
    // this is logical - iterator points to a specific location in memory, index points to some location relative to object start, 
    // so it "survives" dynamic reallocations
    /*
    for (i, &ch) in str.iter().enumerate() {
        // println!("Index: {}, byte: {}", i, ch);
        if ch == b' ' {
            // println!("Found space!");
            if is_space {
                continue;
            }
            is_space = true;
        }
        str[w] = str[i];
        w += 1;
    }*/
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test1() {
        let mut str = AsciiString::from_ascii("asd  ASD".to_string()).unwrap();
        normalize(&mut str);
        assert_eq!(str, "asd ASD");
    }
}

fn main() {
    let mut str = AsciiString::from_ascii("asd  ASD".to_string()).unwrap();
    normalize(&mut str);
    println!("{}", str);
}
