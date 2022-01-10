use std::io;

fn is_palindrome(s: &[u8]) -> bool {
    let mut i = 0;
    let mut j = s.len() - 1;
    while i < j {
        let mut ch1 = s[i];
        let mut ch2 = s[j];
        if (!ch1.is_ascii_alphabetic()) {
            i += 1;
            continue;
        }
        if (!ch2.is_ascii_alphabetic()) {
            j -= 1;
            continue;
        }
        ch1 = ch1.to_ascii_lowercase();
        ch2 = ch2.to_ascii_lowercase();
        if ch1 != ch2 {
            return false;
        }
        i += 1;
        j -= 1;
    }
    return true;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test1() {
        let mut str = String::from("asd asd");
        assert_eq!(is_palindrome(&str.as_bytes()), false);
    }
    #[test]
    fn test2() {
        let mut str = String::from("asd das");
        assert_eq!(is_palindrome(&str.as_bytes()), false);
    }
    #[test]
    fn test3() {
        let mut str = String::from("This should be a palindrome! emOrdilapaebdlouhssiht");
        assert_eq!(is_palindrome(&str.as_bytes()), false);
    }
    #[test]
    fn test4() {
        let mut str = String::from("This is not a palindrome!");
        assert_eq!(is_palindrome(&str.as_bytes()), false);
    }
}

fn main() {
    println!("Enter your line!");
    let mut s = String::new();
    io::stdin()
        .read_line(&mut s)
        .unwrap();
    if (is_palindrome(&s.as_bytes())) {
        println!("Is a palindrome!");
    } else {
        println!("Not a palindrome!");
    }
}
