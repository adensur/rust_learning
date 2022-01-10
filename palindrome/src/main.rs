use std::io;

fn is_palindrome(s: &str) -> bool {
    let mut it = s.chars();
    let mut optional1 = it.next();
    let mut optional2 = it.next_back();
    loop {
        if let (Some(ch1), Some(ch2)) = (optional1, optional2) {
            if !ch1.is_alphabetic() {
                optional1 = it.next();
                continue;
            }
            if !ch2.is_alphabetic() {
                optional2 = it.next_back();
                continue;
            }
            if ch1.to_lowercase().to_string() != ch2.to_lowercase().to_string() {
                println!("lowercase {} != {}", ch1.to_lowercase().to_string(), ch2.to_lowercase().to_string());
                return false;
            }
            optional1 = it.next();
            optional2 = it.next_back();
        } else {
            return true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test1() {
        let str = String::from("asd asd");
        assert_eq!(is_palindrome(&str), false);
    }
    #[test]
    fn test2() {
        let str = String::from("asd das");
        assert_eq!(is_palindrome(&str), false);
    }
    #[test]
    fn test3() {
        let str = String::from("This should be a palindrome! emOrdilapaebdlouhssiht");
        assert_eq!(is_palindrome(&str), false);
    }
    #[test]
    fn test4() {
        let str = String::from("This is not a palindrome!");
        assert_eq!(is_palindrome(&str), false);
    }
    #[test]
    fn test5() {
        let str = String::from("Палиндром!МОРДНИЛАП");
        assert_eq!(is_palindrome(&str), true);
    }
}

fn main() {
    println!("Enter your line!");
    let mut s = String::new();
    io::stdin()
        .read_line(&mut s)
        .unwrap();
    if is_palindrome(&s) {
        println!("Is a palindrome!");
    } else {
        println!("Not a palindrome!");
    }
}
