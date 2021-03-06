use std::collections::HashMap;
use crc::crc32;
use std::env;
use std::fs;
use std::io;
use std::io::Read; // read_exact
use std::str; // str::from_utf8

const MAX_BITS: usize = 16;

#[derive(Debug, PartialEq, Copy, Clone, Eq, Hash)]
struct BitSequence {
    bits: u16,
    len: u8,
}

impl BitSequence {
    fn new(mut bits: u16, len: u8) -> Self {
        assert!(len <= 16);
        if len < 16 {
            let mask = (1 << len) - 1;
            bits = bits & mask;
        }
        Self{ bits: bits, len }
    }

    fn empty() -> Self {
        Self::new(0, 0)
    }

    fn concat(mut self, mut other: Self) -> Self {
        assert!(self.len + other.len <= 16);
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
        let other = BitSequence::new(self.bits, len);
        self.bits >>= len;
        self.len = new_len;
        other
    }

    pub fn reflected(&self) -> Self {
        let mut reflected_bits = 0u16;
        let mut bits = self.bits;
        for bit in 0..self.len {
            if bits & 0b1 > 0 {
                reflected_bits |= 1 << ((self.len - 1) - bit);
            }
            bits >>= 1;
        }
        Self::new(reflected_bits, self.len)
    }

    pub fn from_reflected(mut bits: u16, len: u8) -> Self {
        let mut reflection = 0;
        for bit in 0..len {
            if bits & 0b1 > 0 {
                reflection |= 1 << ((len - 1) - bit);
            }
            bits >>= 1;
        }
        Self::new(reflection, len)
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

    #[test]
    fn test2() {
        let seq = BitSequence::new(0b00111, 5);
        assert_eq!(seq.reflected(), BitSequence::new(0b11100, 5));
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
            self.buf = self.buf.concat(BitSequence::new(buf[0] as u16, 8));
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
                let result = tmp as u32; // take least significant 32 bits
                self.buf = BitSequence::new((tmp >> 32) as u16, self.buf.len);
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
    fn drop_buffer(&mut self) {
        self.buf = BitSequence::empty();
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

#[derive(PartialEq, Debug)]
enum BTypeKind {
    Uncompressed,
    StaticHuffman,
    DynamicHuffman,
    Reserved,
}

struct BType {
    btype: BTypeKind,
}

impl BType {
    fn from_bits(bits: u8) -> Self {
        let btype = match bits {
            0 => BTypeKind::Uncompressed,
            1 => BTypeKind::StaticHuffman,
            2 => BTypeKind::DynamicHuffman,
            3 => BTypeKind::Reserved,
            _ => panic!(),
        };
        Self { btype }
    }

    fn to_string(&self) -> String {
        let slice = match self.btype {
            BTypeKind::Uncompressed => "Uncompressed",
            BTypeKind::StaticHuffman => "StaticHuffman",
            BTypeKind::DynamicHuffman => "DynamicHuffman",
            BTypeKind::Reserved => "Reserved",
        };
        slice.to_string()
    }
}

struct HuffmanDecoder {
    map: HashMap<BitSequence, u16>,
}

impl HuffmanDecoder {
    fn from_lengths(code_lengths: &[u8]) -> Self {
        // See RFC 1951, section 3.2.2.
        // calc counts
        let mut counts: Vec<u16> = Vec::new();
        let mut total_count = 0u16;
        counts.resize(MAX_BITS + 1, 0);
        for &code_len in code_lengths {
            counts[code_len as usize] += 1;
            total_count += 1;
        }
        // calc first symbol for every len
        assert!(total_count <= u16::MAX);
        let mut codes: Vec<u16> = Vec::new();
        codes.resize(MAX_BITS + 1, 0);
        let mut code: u16 = 0;
        for bit_len in 1..=MAX_BITS {
            // starting code for given bitlen
            code = (code + counts[bit_len - 1]) << 1;
            codes[bit_len] = code;
        }
        let mut result: HashMap<BitSequence, u16> = HashMap::new();
        for (symbol, &code_len) in code_lengths.into_iter().enumerate() {
            if code_len == 0 {
                continue;
            }
            let code = codes[code_len as usize];
            // store bits as big-endian
            let bit_sequence = BitSequence::new(code, code_len);
            result.insert(bit_sequence, symbol as u16);
            codes[code_len as usize] += 1;
        }
        Self{map: result}
    }

    fn decode_symbol(&self, seq: BitSequence) -> Option<u16> {
        self.map.get(&seq).cloned()
    }

    fn read_symbol<U: Read>(&self, bit_reader: &mut BitReader<U>) -> Result<u16, io::Error> {
        let mut cur = BitSequence::new(0, 0);
        loop {
            let symbol = bit_reader.read_bits(1)?;
            cur = cur.concat(symbol);
            if let Some(&result) = self.map.get(&BitSequence::from_reflected(cur.bits, cur.len)) {
                return Ok(result);
            }
        }
    }
}

#[cfg(test)]
mod huffman_tests {
    use super::*;
    #[test]
    fn test1() {
        let lengths: &[u8] = &[2, 3, 4, 3, 3, 4, 2];
        let decoder = HuffmanDecoder::from_lengths(lengths);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b00, 2)).unwrap(), 0);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b100, 3)).unwrap(), 1);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b1110, 4)).unwrap(), 2);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b101, 3)).unwrap(), 3);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b110, 3)).unwrap(), 4);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b1111, 4)).unwrap(), 5);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b01, 2)).unwrap(), 6);

        assert_eq!(decoder.decode_symbol(BitSequence::new(0b0, 1)), None);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b10, 2)), None);
        assert_eq!(decoder.decode_symbol(BitSequence::new(0b111, 3)), None);
    }

    #[test]
    fn test2() -> Result<(), io::Error> {
        let decoder = HuffmanDecoder::from_lengths(&[2, 3, 4, 3, 3, 4, 2]);
        let mut data: &[u8] = &[0b10111001, 0b11001010, 0b11101101];
        let mut reader = BitReader::new(&mut data);
        assert_eq!(decoder.read_symbol(&mut reader)?, 1);
        assert_eq!(decoder.read_symbol(&mut reader)?, 2);
        assert_eq!(decoder.read_symbol(&mut reader)?, 3);
        assert_eq!(decoder.read_symbol(&mut reader)?, 6);
        assert_eq!(decoder.read_symbol(&mut reader)?, 0);
        assert_eq!(decoder.read_symbol(&mut reader)?, 2);
        assert_eq!(decoder.read_symbol(&mut reader)?, 4);
        assert!(decoder.read_symbol(&mut reader).is_err());

        Ok(())
    }
}

fn generate_distlen_str(length: u16, dist: u16, history: &[u8]) -> Vec<u8> {
    // dist: 3, len: 10
    // abc_
    // abcabcabcabca
    // take 3 symbols from history
    // repeat 3 times, then take 1 symbol from the last time
    assert!(history.len() >= dist as usize);
    let mut result: Vec<u8> = Vec::with_capacity(length as usize);
    let template = &history[(history.len() - dist as usize)..];
    result.extend(template.iter().cycle().take(length as usize));
    result
}

struct StaticHuffmanDeflateDecoder {
    litlen_decoder: HuffmanDecoder,
    dist_decoder: HuffmanDecoder,
}

fn length_table(symbol: u16) -> (u8, u16) { // offset, length
    match symbol {
        257 => (0, 3),
        258 => (0, 4),
        259 => (0, 5),
        260 => (0, 6),
        261 => (0, 7),
        262 => (0, 8),
        263 => (0, 9),
        264 => (0, 10),
        265 => (1, 11),
        266 => (1, 13),
        267 => (1, 15),
        268 => (1, 17),
        269 => (2, 19),
        270 => (2, 23),
        271 => (2, 27),
        272 => (2, 31),
        273 => (3, 35),
        274 => (3, 43),
        275 => (3, 51),
        276 => (3, 59),
        277 => (4, 67),
        278 => (4, 83),
        279 => (4, 99),
        280 => (4, 115),
        281 => (5, 131),
        282 => (5, 163),
        283 => (5, 195),
        284 => (5, 227),
        285 => (0, 258),
        _ => panic!(),
    }
}

fn dist_table(symbol: u16) -> (u8, u16) { // offset, length
    match symbol {
        0 => (0, 1),
        1 => (0, 2),
        2 => (0, 3),
        3 => (0, 4),
        4 => (1, 5),
        5 => (1, 7),
        6 => (2, 9),
        7 => (2 , 13),
        8 => (3, 17),
        9 => (3, 25),
        10 => (4, 33),
        11 => (4, 49),
        12 => (5, 65),
        13 => (5, 97),
        14 => (6, 129),
        15 => (6, 193),
        16 => (7, 257),
        17 => (7, 385),
        18 => (8, 513),
        19 => (8, 769),
        20 => (9, 1025),
        21 => (9, 1537),
        22 => (10, 2049),
        23 => (10, 3073),
        24 => (11, 4097),
        25 => (11, 6145),
        26 => (12, 8193),
        27 => (12, 12289),
        28 => (13, 16385),
        29 => (13, 24577),
        _ => panic!(),
    }
}

fn read_len_token<T: Read>(symbol: u16, reader: &mut BitReader<T>) -> u16 {
    let (offset, mut length) = length_table(symbol);
    let offset_bits = reader.read_bits(offset).unwrap();
    let actual_offset = BitSequence::new(offset_bits.bits, offset_bits.len).bits;
    length += actual_offset;
    length
}

fn read_dist_token<T: Read>(symbol: u16, reader: &mut BitReader<T>) -> u16 {
    let (offset, mut dist) = dist_table(symbol);
    let offset_bits = reader.read_bits(offset).unwrap();
    let actual_offset = BitSequence::new(offset_bits.bits, offset_bits.len).bits;
    dist += actual_offset;
    dist
}

fn read_block<T: Read>(reader: &mut BitReader<T>, litlen_decoder: &HuffmanDecoder, dist_decoder: &HuffmanDecoder) -> Vec<u8> {
    let mut decoded: Vec<u8> = Vec::new();
    println!("Started block decoding!");
    loop {
        let symbol = litlen_decoder.read_symbol(reader).unwrap();
        match symbol {
            0..=255 => {
                decoded.push(symbol as u8); 
                println!("Read literal: {}", symbol);
            },
            256 => break, // end of block
            257..=285 => {
                // we have len token on our hands!
                let length = read_len_token(symbol, reader);
                let dist_symbol = dist_decoder.read_symbol(reader).unwrap();
                let dist = read_dist_token(dist_symbol, reader);
                let slice = generate_distlen_str(length, dist, decoded.as_ref());
                println!("Read len + dist tokens, len: {}, dist: {}, got str: {}", length, dist, str::from_utf8(&slice).unwrap());
                decoded.extend_from_slice(&slice);
            },
            _ => panic!(),
        }
    }
    println!("Remaining buffer: {:?}", reader.buf);
    reader.drop_buffer();
    println!("Decoded str: {:?}", str::from_utf8(&decoded).unwrap());
    let crc = reader.read_u32().unwrap();
    let isize = reader.read_u32().unwrap();
    println!("crc: {}, decoded crc: {}, isize: {}", crc, crc32::checksum_ieee(decoded.as_ref()), isize);
    assert_eq!(crc, crc::crc32::checksum_ieee(decoded.as_ref()));
    assert!(reader.read_bits(8).is_err());
    decoded
}

impl StaticHuffmanDeflateDecoder {
    fn create_static_litlen_decoder() -> HuffmanDecoder {
        // see https://datatracker.ietf.org/doc/html/rfc1951#section-3.2.6
        let mut lengths: Vec<u8> = Vec::with_capacity(288);
        for _ in 0..=143 {
            lengths.push(8);
        }
        for _ in 144..=255 {
            lengths.push(9);
        }
        for _ in 256..=279 {
            lengths.push(7);
        }
        for _ in 280..=287 {
            lengths.push(8);
        }
        HuffmanDecoder::from_lengths(lengths.as_ref())
    }
    
    fn create_static_dist_decoder() -> HuffmanDecoder {
        // see https://datatracker.ietf.org/doc/html/rfc1951#section-3.2.6
        let mut lengths: Vec<u8> = Vec::with_capacity(32);
        for _ in 0..=31 {
            lengths.push(5);
        }
        HuffmanDecoder::from_lengths(lengths.as_ref())
    }

    fn new() -> Self {
        Self { litlen_decoder: Self::create_static_litlen_decoder(), dist_decoder: Self::create_static_dist_decoder() }
    }

    fn read_block<T: Read>(&self, reader: &mut BitReader<T>) -> Vec<u8> {
        read_block(reader, &self.litlen_decoder, &self.dist_decoder)
    }
}

// kraft-macmillan value
fn kmm(lengths: &[u8]) -> f64 {
    let mut kmm = 0f64;
    for &i in lengths {
        if i > 0u8 {
            kmm += 1f64 / 2f64.powf(i as f64);
        }
    }
    kmm
}

fn read_code_lengths<T: Read>(cl_decoder: &HuffmanDecoder, reader: &mut BitReader<T>, count: usize) -> Vec<u8> {
    let mut result: Vec<u8> = Vec::with_capacity(count);
    let mut i = 0usize;
    while i < count {
        i += 1;
        let bits = cl_decoder.read_symbol(reader).unwrap();
        match bits {
            0..=15 => result.push(bits as u8),
            16 => {
                let offset = (reader.read_bits(2).unwrap().bits + 3) as usize;
                let &prev = result.last().unwrap();
                for _ in 0..offset {
                    i += 1;
                    result.push(prev);
                }
                i -= 1;
            },
            17..=18 => {
                let mut offset = 0;
                if bits == 17 {
                    offset = (reader.read_bits(3).unwrap().bits + 3) as usize;
                } else {
                    offset = (reader.read_bits(7).unwrap().bits + 11) as usize;
                }
                for _ in 0..offset {
                    i += 1;
                    result.push(0);
                }
                i -= 1;
            },
            _ => panic!(),
        }
    }
    result
}

fn decode_dynamic_huffman_deflate<T: Read>(reader: &mut BitReader<T>) -> Vec<u8> {
    let hlit = 257 + reader.read_bits(5).unwrap().bits as usize; // count of litlen lemgth codes
    let hdist = 1 + reader.read_bits(5).unwrap().bits as usize; // count of dist length codes
    let hclen = 4 + reader.read_bits(4).unwrap().bits as usize; // count of CL length codes
    println!("hclen: {}, hlit: {}, hdist: {}", hclen, hlit, hdist);
    const CL_PERMUTATION: [usize; 19] = [16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15];
    let mut cl_lengths: Vec<u8> = vec![0; CL_PERMUTATION.len()];
    // kraft macmillan formula
    for i in 0..hclen {
        let bits = reader.read_bits(3).unwrap().bits;
        println!("Read cl code {} for symbol #{}, symbol value: {}", bits, i, CL_PERMUTATION[i]);
        cl_lengths[CL_PERMUTATION[i]] = bits as u8;
    }
    let cl_kmm = kmm(&cl_lengths);
    println!("Kraft macmillan for cl lengths: {}", cl_kmm);
    assert!((1f64 - cl_kmm).abs() < 1e-7);
    let cl_decoder = HuffmanDecoder::from_lengths(&cl_lengths);
    // !!! This doesnt work!!! [ ]
    // need some more complicated magic from RFC
    let lit_lengths = read_code_lengths(&cl_decoder, reader, hlit);
    assert_eq!(lit_lengths.len(), hlit);
    let dist_lengths = read_code_lengths(&cl_decoder, reader, hdist);
    assert_eq!(dist_lengths.len(), hdist);
    let litlen_kmm = kmm(&lit_lengths);
    let dist_kmm = kmm(&dist_lengths);
    println!("Kraft macmillan for lit lengths: {}, for dist_lengths: {}", litlen_kmm, dist_kmm);
    assert!((1f64 - litlen_kmm).abs() < 1e-7);
    assert!((1f64 - dist_kmm).abs() < 1e-7);
    let lit_decoder = HuffmanDecoder::from_lengths(&lit_lengths);
    let dist_decoder = HuffmanDecoder::from_lengths(&dist_lengths);
    read_block(reader, &lit_decoder, &dist_decoder)
}

#[cfg(test)]
mod static_deflate_tests {
    use super::*;
    #[test]
    fn test1() {
        let decoder = StaticHuffmanDeflateDecoder::new();
        let buf: &[u8] = &[0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x57, 0x28, 0xcf, 0x2f, 0xca, 0x49, 0x51,
            0xe4, 0x02, 0x00, 0x41, 0xe4, 0xa9, 0xb2, 0x0d, 0x00, 0x00, 0x00];
        let mut reader = BitReader::new(buf);
        let is_last = reader.read_bits(1).unwrap().bits > 0;
        assert_eq!(is_last, true);
        let btype = BType::from_bits(reader.read_bits(2).unwrap().bits as u8);
        assert_eq!(btype.btype, BTypeKind::StaticHuffman);
        let decoded = decoder.read_block(&mut reader);
        assert_eq!(decoded, "Hello world!\n".as_bytes())
    }

    #[test]
    fn test2() {
        let decoder = StaticHuffmanDeflateDecoder::new();
        let buf: &[u8] = &[0x4b, 0x4c, 0x44, 0x06, 0x5c, 0x00, 0xa1, 0x84, 0xf7, 0xb9, 0x0f, 0x00, 0x00, 0x00,];
        let mut reader = BitReader::new(buf);
        let is_last = reader.read_bits(1).unwrap().bits > 0;
        assert_eq!(is_last, true);
        let btype = BType::from_bits(reader.read_bits(2).unwrap().bits as u8);
        assert_eq!(btype.btype, BTypeKind::StaticHuffman);
        let decoded = decoder.read_block(&mut reader);
        assert_eq!(decoded, "aaaaaaaaaaaaaa\n".as_bytes())
    }

    #[test]
    fn test3() {
        let decoder = StaticHuffmanDeflateDecoder::new();
        let buf: &[u8] = &[0x8b, 0x2e, 0x48, 0x4c, 0xce, 0x4e, 0x4c, 0x4f, 0x8d, 0xe5, 0xca, 0x4b, 0xcc, 0x4d, 0x55, 0xb0,
        0x55, 0x50, 0x2a, 0xca, 0x2c, 0x48, 0xaf, 0xca, 0x2c, 0x50, 0xe2, 0x2a, 0x4b, 0x2d, 0x2a, 0xce, 0xcc, 0xcf, 0x03,
        0x89, 0x19, 0xe8, 0x19, 0xea, 0x19, 0x28, 0x01, 0x00, 0xfd, 0x29, 0x36, 0x39, 0x2c, 0x00, 0x00, 0x00,];
        let mut reader = BitReader::new(buf);
        let is_last = reader.read_bits(1).unwrap().bits > 0;
        assert_eq!(is_last, true);
        let btype = BType::from_bits(reader.read_bits(2).unwrap().bits as u8);
        assert_eq!(btype.btype, BTypeKind::StaticHuffman);
        let decoded = decoder.read_block(&mut reader);
        assert_eq!(decoded, "[package]\nname = \"ripgzip\"\nversion = \"0.1.0\"".as_bytes());
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
    if fname {
        let original_filename = reader.read_cstr().unwrap();
        println!("original filename: {}", original_filename);
    }

    // start reading deflate blocks
    let is_last = reader.read_bits(1).unwrap().bits > 0;
    println!("Is last: {}", is_last);
    let btype = BType::from_bits(reader.read_bits(2).unwrap().bits as u8);
    println!("Btype: {:?}", btype.to_string());
    match btype.btype {
        BTypeKind::StaticHuffman => {
            let decoder = StaticHuffmanDeflateDecoder::new();
            let decoded = decoder.read_block(&mut reader);
            println!("Read str: {}", str::from_utf8(&decoded).unwrap());
        },
        BTypeKind::DynamicHuffman => {
            let decoded = decode_dynamic_huffman_deflate(&mut reader);
            println!("Read str: {}", str::from_utf8(&decoded).unwrap());
        }
        _ => panic!(),
    }
}
