use std::env;
use std::fs;
use std::io;
use std::io::Read; // read_exact

#[derive(Debug, PartialEq, Copy, Clone)]
struct BitSequence {
    bits: u16,
    len: u8,
}

impl BitSequence {
    fn new(mut bits: u8, len: u8) -> Self {
        assert!(len <= 8);
        if len < 8 {
            let mask = (1 << len) - 1;
            bits = bits & mask;
        }
        Self{ bits: bits as u16, len }
    }

    fn empty() -> Self {
        Self::new(0, 0)
    }

    fn concat(mut self, mut other: Self) -> Self {
        debug_assert!(self.len + other.len <= 16);
        other.bits <<= self.len;
        self.bits += other.bits;
        self.len += other.len;
        self
        // NB: result must not be larger than 16 bits.
    }

    fn take(&mut self, len: u8) -> Self {
        // cuts off and returns leftmost len bits
        assert!(len <= self.len);
        let new_len = self.len - len;
        let other = BitSequence::new((self.bits) as u8, len);
        self.bits >>= len;
        self.len = new_len;
        other
    }
}

/*
fn<T> reflect(number: T) -> T {
    let mut reflection = 0;
    for bit in 0..len {
        if bits & 0b1 > 0 {
            reflection |= 1 << ((len - 1) - bit);
        }
        bits >>= 1;
    }
}*/

#[cfg(test)]
mod bitsequence_tests {
    use super::*;
    #[test]
    fn test1() {
        let mut seq = BitSequence::new(0b1011, 4);
        assert_eq!(seq, BitSequence::new(0b101011, 4));
        let seq2 = seq.take(2);
        assert_eq!(seq2, BitSequence::new(0b11, 2));
        assert_eq!(seq, BitSequence::new(0b10, 2));

        assert_eq!(seq2.concat(seq2), BitSequence::new(0b1111, 4));

        let seq = BitSequence::new(0b1000, 4);
        let seq2 = BitSequence::new(0b0001, 4);
        let mut seq3 = seq.concat(seq2);
        assert_eq!(seq3, BitSequence::new(0b00011000, 8));
        let seq4 = seq3.take(4);
        assert_eq!(seq4, BitSequence::new(0b1000, 4));
        assert_eq!(seq3, BitSequence::new(0b0001, 4));
    }
}

/*
    allows to read specified number of bits (1-8) from stream
 */
struct BitReader<T>
where
    T: Read,
{
    reader: T,
    buf: BitSequence,
}

impl<T> BitReader<T>
where
    T: Read
{
    fn new(reader: T) -> Self {
        Self{reader, buf: BitSequence::empty()}
    }

    fn read_bits(&mut self, len: u8) -> io::Result<BitSequence> {
        assert!(len <= 8);
        if len <= self.buf.len {
            return Ok(self.buf.take(len));
        }
        let mut buf = [0u8; 1];
        let res = self.reader.read_exact(&mut buf);
        res.and_then(|_| {
            self.buf = self.buf.concat(BitSequence::new(buf[0], 8));
            Ok(self.buf.take(len))
        })
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let mut tmp: u64 = self.buf.bits as u64;
        let mut buf = [0u8; 4];
        self.reader
            .read_exact(&mut buf)
            .and_then(|_| {
                let mut shift = self.buf.len;
                for i in 0..4 {
                    tmp += (buf[i] as u64) << shift;
                    shift += 8;
                }
                let result = (tmp >> self.buf.len) as u32;
                self.buf = BitSequence::new(tmp as u8, self.buf.len);
                Ok(result)
            })
    }

    fn read_cstr(&mut self) -> io::Result<String> {
        let mut result = String::new();
        loop {
            let r = self.read_bits(8);
            match r {
                Ok(symbol) => {
                    if symbol.bits == 0 {
                        return Ok(result);
                    }
                    result.push(symbol.bits as u8 as char);
                }
                Err(error) => return Err(error)
            }
        }
    }
}

#[cfg(test)]
mod bitreader_tests {
    use super::*;
    #[test]
    fn test1() {
        let stream = "Hello world!".as_bytes();
        let mut reader = BitReader::new(stream);
        assert_eq!(reader.read_bits(1).unwrap(), BitSequence::new(0b0, 1));
        assert_eq!(reader.read_bits(2).unwrap(), BitSequence::new(0b00, 2));
        assert_eq!(reader.read_bits(3).unwrap(), BitSequence::new(0b001, 3));
        assert_eq!(reader.read_bits(4).unwrap(), BitSequence::new(0b0101, 4));
        assert_eq!(reader.read_bits(8).unwrap(), BitSequence::new(0b00011001, 8));
    }
    #[test]
    fn test2() {
        // numbers are serialized in the same way as text!
        let stream: &[u8] = &[0b01001000, 0b01100101, 0b01101100];
        let mut reader = BitReader::new(stream);
        assert_eq!(reader.read_bits(1).unwrap(), BitSequence::new(0b0, 1));
        assert_eq!(reader.read_bits(2).unwrap(), BitSequence::new(0b00, 2));
        assert_eq!(reader.read_bits(3).unwrap(), BitSequence::new(0b001, 3));
        assert_eq!(reader.read_bits(4).unwrap(), BitSequence::new(0b0101, 4));
        assert_eq!(reader.read_bits(8).unwrap(), BitSequence::new(0b00011001, 8));
    }
    #[test]
    fn test3() {
        // test little endian number read
        let stream: &[u8] = &[0x00, 0xdf, 0xf7, 0x61]; // little-endian hex for 1643634432
        let mut reader = BitReader::new(stream);
        assert_eq!(reader.read_u32().unwrap(), 1643634432);
    }
    #[test]
    fn test4() {
        // test reading zero-terminated cstr
        let stream: &[u8] = &[0x69, 0x6e, 0x70, 0x75, 0x74, 0x2e, 0x74, 0x78, 0x74, 0x00, 0x48];
        let mut reader = BitReader::new(stream);
        assert_eq!(reader.read_cstr().unwrap(), "input.txt");
        assert_eq!(reader.read_bits(8).unwrap(), BitSequence::new(0x48, 8));
        assert!(reader.read_bits(8).is_err());
    }
}

enum OsKind {
    Fat, // osflag = 0
    Omega, // 1
    Vms, // 2
    Unix, // 3
    Vm, // 4
    Atari, // 5
    Hpfs, // 6
    Mac, // 7, stands for old, pre-unix macs
    Z, // 8
    CPM, // 9
    TOPS, // 10
    NTFS, // 11
    QDOS, // 12
    Acorn, // 13
    Unknown, // 255
}

struct Os {
    kind: OsKind
}

impl Os {
    fn from_bits(bits: u8) -> Os {
        let kind = match bits {
            0 => OsKind::Fat,
            1 => OsKind::Omega,
            2 => OsKind::Vms,
            3 => OsKind::Unix,
            4 => OsKind::Vm,
            5 => OsKind::Atari,
            6 => OsKind::Hpfs,
            7 => OsKind::Mac,
            8 => OsKind::Z,
            9 => OsKind::CPM,
            10 => OsKind::TOPS,
            11 => OsKind::NTFS,
            12 => OsKind::QDOS,
            13 => OsKind::Acorn,
            255 => OsKind::Unknown,
            _ => panic!()
        };
        Self { kind }
    }

    fn to_string(&self) -> String {
        let slice = match self.kind {
            OsKind::Fat => "Fat",
            OsKind::Omega => "Omega",
            OsKind::Vms => "Vms",
            OsKind::Unix => "Unix",
            OsKind::Vm => "Vm",
            OsKind::Atari => "Atari",
            OsKind::Hpfs => "Hpfs",
            OsKind::Mac => "Mac",
            OsKind::Z => "Z",
            OsKind::CPM => "CPM",
            OsKind::TOPS => "TOPS",
            OsKind::NTFS => "NTFS",
            OsKind::QDOS => "QDOS",
            OsKind::Acorn => "Acorn",
            OsKind::Unknown => "Unknown",
        };
        slice.to_string()
    }
}

fn main() {
    let args: Vec<_> = env::args().collect();
    let filename = args[1].clone();
    let file = fs::File::open(filename).unwrap();
    let mut reader = BitReader::new(file);
    let seq = reader.read_bits(8).unwrap();
    assert_eq!(seq.bits, 0x1f); // first magic header
    let seq = reader.read_bits(8).unwrap();
    assert_eq!(seq.bits, 0x8b); // first magic header
    let seq = reader.read_bits(8).unwrap();
    assert_eq!(seq.bits, 0x08); // compression method, 0x08 stands for "deflate"
    let ftext = reader.read_bits(1).unwrap().bits > 0;
    println!("ftext: {}", ftext);
    let fhcrc = reader.read_bits(1).unwrap().bits > 0;
    println!("fhcrc: {}", fhcrc);
    let fextra = reader.read_bits(1).unwrap().bits > 0;
    println!("fextra: {}", fextra);
    let fname = reader.read_bits(1).unwrap().bits > 0;
    println!("fname: {}", fname);
    let fcomment = reader.read_bits(1).unwrap().bits > 0;
    println!("fcomment: {}", fcomment);
    let _ = reader.read_bits(3).unwrap(); // trailing 3 zeroes of FLAGS
    let mtime = reader.read_u32().unwrap();
    println!("mtime: {}", mtime);
    let xfl = reader.read_bits(8).unwrap();
    println!("xfl: {}", xfl.bits);
    let os = Os::from_bits(reader.read_bits(8).unwrap().bits as u8);
    println!("os: {}", os.to_string());
    let original_filename = reader.read_cstr().unwrap();
    println!("original filename: {}", original_filename);
    println!("Hello world!");
}
