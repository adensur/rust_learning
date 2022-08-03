#![feature(map_first_last)]
use std::collections::BTreeSet;

#[derive(PartialEq, Debug)]
enum MyError {
    OutOfMemory,
    DoubleFree,
    MalformedFree,
}

struct Allocator {
    allocated_idxs: Vec<bool>,
    free_idxs: BTreeSet<u16>,
}

impl Allocator {
    pub fn new() -> Self {
        Self {
            allocated_idxs: Vec::new(),
            free_idxs: BTreeSet::new(),
        }
    }

    pub fn alloc(self: &mut Self) -> Result<u16, MyError> {
        if let Some(idx) = self.free_idxs.pop_first() {
            return Ok(idx);
        }
        if self.allocated_idxs.len() as u16 == std::u16::MAX {
            return Err(MyError::OutOfMemory);
        }
        self.allocated_idxs.push(true);
        Ok(self.allocated_idxs.len() as u16 - 1)
    }

    pub fn free(self: &mut Self, idx: u16) -> Result<(), MyError> {
        if let Some(is_allocated) = self.allocated_idxs.get(idx as usize) {
            if *is_allocated == false {
                return Err(MyError::DoubleFree);
            }
            self.allocated_idxs[idx as usize] = false;
            self.free_idxs.insert(idx);
            return Ok(());
        }
        Err(MyError::MalformedFree)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test1() {
        let mut alloc = Allocator::new();
        assert_eq!(Ok(0), alloc.alloc());
        assert_eq!(Ok(1), alloc.alloc());
        assert_eq!(Ok(()), alloc.free(1));
        assert_eq!(Ok(()), alloc.free(0));
        assert_eq!(Ok(0), alloc.alloc());
    }
}

fn main() {
    println!("Hello, world!");
}
