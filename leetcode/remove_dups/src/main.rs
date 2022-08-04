/*
    Recursively remove any character with concesutive count > 1
    "abba" -> ""
*/

fn remove_dups(mut s: Vec<u8>) -> Vec<u8> {
    if s.len() <= 1 {
        return s;
    }
    let mut write_idx = 0;
    let mut read_idx = 0;
    while read_idx < s.len() {
        let mut is_duplicate = false;
        let ch = s[read_idx];
        read_idx += 1;
        while read_idx < s.len() && s[read_idx] == ch {
            is_duplicate = true;
            read_idx += 1;
        }
        // read_idx now points to the next non-equal char
        if is_duplicate {
            continue;
        }
        if write_idx > 0 && ch == s[write_idx - 1] {
            write_idx -= 1;
            continue;
        }
        s[write_idx] = s[read_idx - 1];
        write_idx += 1;
    }
    s.resize(write_idx, u8::default());
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test1() {
        let mut s: Vec<u8> = "aab".into();
        s = remove_dups(s);
        let expected: Vec<u8> = "b".into();
        assert_eq!(
            s,
            expected,
            r#"expected "{}", found "{}""#,
            String::from_utf8(expected.clone()).unwrap(),
            String::from_utf8(s.clone()).unwrap(),
        );
    }

    #[test]
    fn test2() {
        let mut s: Vec<u8> = "aaab".into();
        s = remove_dups(s);
        let expected: Vec<u8> = "b".into();
        assert_eq!(
            s,
            expected,
            r#"expected "{}", found "{}""#,
            String::from_utf8(expected.clone()).unwrap(),
            String::from_utf8(s.clone()).unwrap(),
        );
    }

    #[test]
    fn test3() {
        let mut s: Vec<u8> = "ab".into();
        s = remove_dups(s);
        let expected: Vec<u8> = "ab".into();
        assert_eq!(
            s,
            expected,
            r#"expected "{}", found "{}""#,
            String::from_utf8(expected.clone()).unwrap(),
            String::from_utf8(s.clone()).unwrap(),
        );
    }

    #[test]
    fn test4() {
        let mut s: Vec<u8> = "abba".into();
        s = remove_dups(s);
        let expected: Vec<u8> = "".into();
        assert_eq!(
            s,
            expected,
            r#"expected "{}", found "{}""#,
            String::from_utf8(expected.clone()).unwrap(),
            String::from_utf8(s.clone()).unwrap(),
        );
    }
}

fn main() {
    println!("Hello, world!");
}
