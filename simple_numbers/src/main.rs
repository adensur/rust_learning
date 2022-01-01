const TARGET : i32 = 10_000_000;


fn main() {
    let mut vec: Vec<i32> = Vec::new();
    for i in 2..=TARGET {
        let mut flag = false;
        for prime in &vec {
            if i % prime == 0 {
                flag = true;
                break;
            }
            if *prime > ((i as f64).sqrt() as i32) {
                break;
            }
        }
        if !flag {
            vec.push(i);
        }
    }
    if let Some(prime) = vec.last() {
        println!("{}", prime);
    }
}
