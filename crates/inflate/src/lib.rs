#![forbid(unsafe_code)]

//! A pure-Rust inflater (RFC 1951) for gzip streams, with checkpoints. gzip
//! has no random access: reaching a byte means decoding everything before it.
//! At a block boundary the inflater can record where the input stands and the
//! last 32 KiB of output, which is all a later decode needs to resume there.
//! No C binding and no unsafe code.
//!
//! [`Inflater::new`] decodes a stream from its start, taking a [`Checkpoint`]
//! at the first block boundary after every so many bytes of output;
//! [`Inflater::resume`] picks up at one, given the input positioned at its
//! `in_offset`. Both read as a [`std::io::Read`] of the uncompressed bytes.
//!
//! The hot loop is built the way miniz's is: preconditions checked once per
//! symbol, the bit buffer and input cursor in locals, an 11-bit lookup with
//! a bit-by-bit fallback. It decodes a real layer at `miniz_oxide`'s speed.

use std::io::{self, Read};

/// The LZ77 window a block may reference, and so what a checkpoint carries.
pub const WINDOW: usize = 32 * 1024;
const RING: usize = 2 * WINDOW;
const FAST_BITS: u32 = 11;
/// Input bytes the fast loop needs ahead: three four-byte fills plus slack.
const FAST_INPUT: usize = 16;
/// Output room the fast loop needs: the longest match.
const FAST_OUTPUT: u64 = 259;
const FAST_SIZE: usize = 1 << FAST_BITS;
const RING_MASK: u64 = RING as u64 - 1;
const MAX_CODE_BITS: usize = 15;

const LENGTH_BASE: [u16; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LENGTH_EXTRA: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DIST_BASE: [u16; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
];
const DIST_EXTRA: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
/// The order code-length code lengths are transmitted in.
const CODE_LENGTH_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

/// Where a later inflate may resume: the input byte and bit the next block
/// starts at, the output offset it corresponds to, and the window it needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// Offset in the stream of the byte holding the next block's first bit.
    pub in_offset: u64,
    /// Bits of that byte already consumed, 0 to 7.
    pub bit: u8,
    pub out_offset: u64,
    /// The last [`WINDOW`] bytes of output before `out_offset`, fewer near
    /// the start of the stream.
    pub window: Vec<u8>,
}

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("deflate: {what}"))
}

fn unexpected_end() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "deflate: unexpected end of stream",
    )
}

/// The ring slot an output offset lands in.
fn slot(offset: u64) -> usize {
    usize::try_from(offset & RING_MASK).unwrap_or(0)
}

/// A canonical Huffman code: a fast table for codes up to [`FAST_BITS`] long
/// and the per-length counts and sorted symbols for the rest.
struct Huffman {
    fast: Box<[u16; FAST_SIZE]>,
    counts: [u16; MAX_CODE_BITS + 1],
    symbols: Vec<u16>,
}

impl Huffman {
    fn new(lengths: &[u8]) -> io::Result<Self> {
        let mut counts = [0u16; MAX_CODE_BITS + 1];
        for &length in lengths {
            counts[usize::from(length)] += 1;
        }
        counts[0] = 0;
        let mut left: i32 = 1;
        for &count in &counts[1..] {
            left = (left << 1) - i32::from(count);
            if left < 0 {
                return Err(invalid("over-subscribed code set"));
            }
        }
        let mut offsets = [0u16; MAX_CODE_BITS + 2];
        for length in 1..=MAX_CODE_BITS {
            offsets[length + 1] = offsets[length] + counts[length];
        }
        let mut symbols = vec![0u16; lengths.len()];
        for (symbol, &length) in lengths.iter().enumerate() {
            if length != 0 {
                let slot = &mut offsets[usize::from(length)];
                symbols[usize::from(*slot)] = u16::try_from(symbol).unwrap_or(u16::MAX);
                *slot += 1;
            }
        }
        // Canonical codes, then the fast table keyed by the bit-reversed
        // code padded with every possible continuation.
        let mut next_code = [0u32; MAX_CODE_BITS + 2];
        let mut code = 0u32;
        for length in 1..=MAX_CODE_BITS {
            code = (code + u32::from(counts[length - 1])) << 1;
            next_code[length] = code;
        }
        let mut fast = Box::new([0u16; FAST_SIZE]);
        for (symbol, &length) in lengths.iter().enumerate() {
            let length = u32::from(length);
            if length == 0 || length > FAST_BITS {
                continue;
            }
            let code = next_code[length as usize];
            next_code[length as usize] += 1;
            let reversed = code.reverse_bits() >> (32 - length);
            let entry = u16::try_from((symbol << 4) | length as usize).unwrap_or(0);
            let mut index = reversed;
            while index < u32::try_from(FAST_SIZE).unwrap_or(u32::MAX) {
                fast[index as usize % FAST_SIZE] = entry;
                index += 1 << length;
            }
        }
        Ok(Self {
            fast,
            counts,
            symbols,
        })
    }

    /// The fixed literal/length code of RFC 1951 3.2.6.
    fn fixed_literals() -> io::Result<Self> {
        let mut lengths = [8u8; 288];
        lengths[144..256].fill(9);
        lengths[256..280].fill(7);
        Self::new(&lengths)
    }

    fn fixed_distances() -> io::Result<Self> {
        Self::new(&[5u8; 30])
    }
}

/// The input side: bytes read in bulk and handed out as bits, least
/// significant first, counting what was consumed for the checkpoints.
struct BitReader<R: Read> {
    input: R,
    buf: Vec<u8>,
    len: usize,
    pos: usize,
    bits: u64,
    nbits: u32,
    /// Bytes pulled from `input` so far, header and trailer included.
    consumed: u64,
    eof: bool,
}

impl<R: Read> BitReader<R> {
    fn new(input: R) -> Self {
        Self {
            input,
            buf: vec![0; 64 * 1024],
            len: 0,
            pos: 0,
            bits: 0,
            nbits: 0,
            consumed: 0,
            eof: false,
        }
    }

    /// Bytes of the chunk not yet moved into the bit buffer.
    #[inline]
    fn chunk_left(&self) -> usize {
        self.len - self.pos
    }

    /// Slides the chunk's unread tail to its front and reads more behind it,
    /// so [`FAST_INPUT`] bytes are ahead unless the input has ended.
    fn top_up(&mut self) -> io::Result<()> {
        if self.chunk_left() >= FAST_INPUT || self.eof {
            return Ok(());
        }
        self.buf.copy_within(self.pos..self.len, 0);
        self.len -= self.pos;
        self.pos = 0;
        while self.len < self.buf.len() {
            match self.input.read(&mut self.buf[self.len..]) {
                Ok(0) => {
                    self.eof = true;
                    break;
                }
                Ok(n) => self.len += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// The fast loop's chunk top-up: the cursor `p` it holds is written back,
    /// the chunk slid and refilled, and the new cursor returned. The loop
    /// checks what is ahead of it afterwards.
    fn top_up_from(&mut self, p: usize) -> io::Result<usize> {
        self.consumed += (p - self.pos) as u64;
        self.pos = p;
        self.top_up()?;
        Ok(self.pos)
    }

    fn next_input_byte(&mut self) -> io::Result<Option<u8>> {
        if self.pos == self.len {
            self.len = loop {
                match self.input.read(&mut self.buf) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            };
            self.pos = 0;
            if self.len == 0 {
                return Ok(None);
            }
        }
        let byte = self.buf[self.pos];
        self.pos += 1;
        self.consumed += 1;
        Ok(Some(byte))
    }

    /// Tops the bit buffer up to at least 56 bits, as far as the input goes:
    /// seven bytes at once from the buffered input, one at a time at its end.
    fn refill(&mut self) -> io::Result<()> {
        while self.nbits < 56 {
            if self.pos + 8 <= self.len {
                let bytes = usize::try_from((63 - self.nbits) / 8).unwrap_or(0);
                // Only the bytes that fit go in; the word's high bytes stay
                // unread in the input for the next refill.
                let word = u64::from_le_bytes(
                    self.buf[self.pos..self.pos + 8]
                        .try_into()
                        .map_err(|_| invalid("short read"))?,
                ) & ((1u64 << (bytes * 8)) - 1);
                self.bits |= word << self.nbits;
                self.pos += bytes;
                self.consumed += bytes as u64;
                self.nbits += u32::try_from(bytes * 8).unwrap_or(0);
                continue;
            }
            match self.next_input_byte()? {
                Some(byte) => {
                    self.bits |= u64::from(byte) << self.nbits;
                    self.nbits += 8;
                }
                None => return Ok(()),
            }
        }
        Ok(())
    }

    fn take(&mut self, n: u32) -> io::Result<u32> {
        if self.nbits < n {
            self.refill()?;
            if self.nbits < n {
                return Err(unexpected_end());
            }
        }
        let value = u32::try_from(self.bits & ((1u64 << n) - 1))
            .map_err(|_| invalid("bit run too long"))?;
        self.bits >>= n;
        self.nbits -= n;
        Ok(value)
    }

    /// Drops the bits left in the current byte, for the byte-aligned parts.
    fn align(&mut self) {
        let drop = self.nbits % 8;
        self.bits >>= drop;
        self.nbits -= drop;
    }

    /// A whole byte, from the bit buffer when aligned bits are pending and
    /// straight from the input otherwise; `None` at the end of input.
    fn next_byte_or_end(&mut self) -> io::Result<Option<u8>> {
        if self.nbits >= 8 {
            return Ok(Some(u8::try_from(self.take(8)?).unwrap_or(u8::MAX)));
        }
        self.next_input_byte()
    }

    fn next_byte(&mut self) -> io::Result<u8> {
        self.next_byte_or_end()?.ok_or_else(unexpected_end)
    }

    /// The bits consumed so far, as the byte and bit a checkpoint records.
    fn position(&self) -> (u64, u8) {
        let consumed_bits = self.consumed * 8 - u64::from(self.nbits);
        (
            consumed_bits / 8,
            u8::try_from(consumed_bits % 8).unwrap_or(0),
        )
    }

    /// Starts reading at `bit` of the next byte, for a resume.
    fn skip_bits_of_next_byte(&mut self, bit: u8) -> io::Result<()> {
        let byte = self.next_byte()?;
        self.bits = u64::from(byte) >> bit;
        self.nbits = 8 - u32::from(bit);
        Ok(())
    }

    /// One symbol: a table lookup nearly always, the canonical walk for a
    /// code longer than the table or at the very end of the input.
    #[inline]
    fn decode(&mut self, table: &Huffman) -> io::Result<u16> {
        // A short buffer near the end of input is fine: the last symbols of a
        // stream can be shorter than the table width.
        if self.nbits < FAST_BITS {
            self.refill()?;
        }
        let entry = table.fast
            [usize::try_from(self.bits & (FAST_SIZE as u64 - 1)).unwrap_or(0) % FAST_SIZE];
        let length = u32::from(entry & 0xf);
        if length != 0 && length <= self.nbits {
            self.bits >>= length;
            self.nbits -= length;
            return Ok(entry >> 4);
        }
        self.decode_slow(table)
    }

    /// One bit at a time over the canonical code (puff's decode).
    #[cold]
    fn decode_slow(&mut self, table: &Huffman) -> io::Result<u16> {
        let mut code: i64 = 0;
        let mut first: i64 = 0;
        let mut index: i64 = 0;
        for length in 1..=MAX_CODE_BITS {
            code |= i64::from(self.take(1)?);
            let count = i64::from(table.counts[length]);
            if code - count < first {
                let at =
                    usize::try_from(index + (code - first)).map_err(|_| invalid("bad code"))?;
                return table
                    .symbols
                    .get(at)
                    .copied()
                    .ok_or_else(|| invalid("bad code"));
            }
            index += count;
            first += count;
            first <<= 1;
            code <<= 1;
        }
        Err(invalid("invalid Huffman code"))
    }
}

/// Four more bytes into the fast loop's bit buffer when it holds fewer than
/// 30 bits; the loop has checked the chunk holds them. A macro over the
/// loop's locals rather than a function taking them by reference, so they
/// stay in registers.
macro_rules! fill32 {
    ($bits:ident, $p:ident, $b:ident, $n:ident) => {
        if $n < 30 {
            let word = $bits.buf[$p..$p + 4]
                .try_into()
                .map_or(0, u32::from_le_bytes);
            $b |= u64::from(word) << $n;
            $p += 4;
            $n += 32;
        }
    };
}

/// A code longer than the table, walked on the reader itself: the loop's
/// locals are written back before and reloaded after.
macro_rules! slow {
    ($bits:ident, $table:expr, $p:ident, $b:ident, $n:ident) => {{
        $bits.consumed += ($p - $bits.pos) as u64;
        $bits.pos = $p;
        $bits.bits = $b;
        $bits.nbits = $n;
        let symbol = $bits.decode_slow($table)?;
        $b = $bits.bits;
        $n = $bits.nbits;
        $p = $bits.pos;
        symbol
    }};
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    /// At a gzip member header.
    MemberStart,
    /// Between two deflate blocks, where a checkpoint may be taken.
    BlockStart,
    Stored {
        remaining: u16,
    },
    Huffman,
    BlockEnd,
    /// After the final block: the trailer, then either another member or the end.
    MemberEnd,
    Done,
}

/// The two codes of a Huffman block.
struct Block {
    literals: Huffman,
    distances: Huffman,
}

/// Inflates a gzip stream (one member or several) read from `input`, as a
/// [`Read`] of the uncompressed bytes.
pub struct Inflater<R: Read> {
    bits: BitReader<R>,
    ring: Box<[u8; RING]>,
    /// Total output produced, and how much of it the caller has read.
    pos: u64,
    served: u64,
    state: State,
    final_block: bool,
    /// Bits of the first input byte to drop when resuming at a checkpoint.
    resume_bit: Option<u8>,
    block: Option<Block>,
    checkpoint_every: u64,
    last_checkpoint: u64,
    checkpoints: Vec<Checkpoint>,
}

impl<R: Read> Inflater<R> {
    /// Inflates from the start of a gzip stream, recording a checkpoint at the
    /// first block boundary after every `checkpoint_every` bytes of output;
    /// `u64::MAX` records none.
    ///
    /// # Errors
    ///
    /// Only when the 64 KiB ring cannot be allocated, which never happens in
    /// practice; the stream itself is read lazily.
    pub fn new(input: R, checkpoint_every: u64) -> io::Result<Self> {
        // Built on the heap: a 64 KiB array would land on the stack first.
        let ring = vec![0u8; RING]
            .into_boxed_slice()
            .try_into()
            .map_err(|_| invalid("ring allocation"))?;
        Ok(Self {
            bits: BitReader::new(input),
            ring,
            pos: 0,
            served: 0,
            state: State::MemberStart,
            final_block: false,
            resume_bit: None,
            block: None,
            checkpoint_every,
            last_checkpoint: 0,
            checkpoints: Vec::new(),
        })
    }

    /// Resumes at `checkpoint`, `input` being positioned at its `in_offset`.
    ///
    /// # Errors
    ///
    /// As [`Inflater::new`].
    pub fn resume(input: R, checkpoint: &Checkpoint) -> io::Result<Self> {
        let mut inflater = Self::new(input, u64::MAX)?;
        inflater.bits.consumed = checkpoint.in_offset;
        inflater.pos = checkpoint.out_offset;
        inflater.served = checkpoint.out_offset;
        inflater.state = State::BlockStart;
        inflater.resume_bit = Some(checkpoint.bit);
        let start = checkpoint
            .out_offset
            .saturating_sub(checkpoint.window.len() as u64);
        for (i, &byte) in checkpoint.window.iter().enumerate() {
            inflater.ring[slot(start + i as u64)] = byte;
        }
        Ok(inflater)
    }

    /// Total uncompressed bytes produced so far.
    pub fn position(&self) -> u64 {
        self.pos
    }

    pub fn into_checkpoints(self) -> Vec<Checkpoint> {
        self.checkpoints
    }

    /// Reads and discards `n` bytes of output.
    ///
    /// # Errors
    ///
    /// An input read error, a malformed stream, or a stream that ends before
    /// `n` bytes were produced.
    pub fn skip(&mut self, mut n: u64) -> io::Result<()> {
        let mut scratch = [0u8; 8192];
        while n > 0 {
            let want = usize::try_from(n).map_or(scratch.len(), |n| n.min(scratch.len()));
            let read = self.read(&mut scratch[..want])?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "deflate: stream ends before the requested offset",
                ));
            }
            n -= read as u64;
        }
        Ok(())
    }

    fn read_member_header(&mut self) -> io::Result<()> {
        let id1 = self.bits.next_byte()?;
        let id2 = self.bits.next_byte()?;
        self.read_member_header_after_magic(id1, id2)
    }

    fn read_member_header_after_magic(&mut self, id1: u8, id2: u8) -> io::Result<()> {
        if (id1, id2) != (0x1f, 0x8b) {
            return Err(invalid("not a gzip stream"));
        }
        if self.bits.next_byte()? != 8 {
            return Err(invalid("unsupported gzip compression method"));
        }
        let flags = self.bits.next_byte()?;
        for _ in 0..6 {
            self.bits.next_byte()?; // mtime, xfl, os
        }
        if flags & 0x04 != 0 {
            let extra =
                u16::from(self.bits.next_byte()?) | (u16::from(self.bits.next_byte()?) << 8);
            for _ in 0..extra {
                self.bits.next_byte()?;
            }
        }
        if flags & 0x08 != 0 {
            while self.bits.next_byte()? != 0 {}
        }
        if flags & 0x10 != 0 {
            while self.bits.next_byte()? != 0 {}
        }
        if flags & 0x02 != 0 {
            self.bits.next_byte()?;
            self.bits.next_byte()?;
        }
        Ok(())
    }

    fn take_checkpoint(&mut self) {
        if self.pos == 0 || self.pos - self.last_checkpoint < self.checkpoint_every {
            return;
        }
        let (in_offset, bit) = self.bits.position();
        let len = usize::try_from(self.pos.min(WINDOW as u64)).unwrap_or(WINDOW);
        let window = (0..len)
            .map(|i| self.ring[slot(self.pos - len as u64 + i as u64)])
            .collect();
        self.checkpoints.push(Checkpoint {
            in_offset,
            bit,
            out_offset: self.pos,
            window,
        });
        self.last_checkpoint = self.pos;
    }

    fn start_block(&mut self) -> io::Result<()> {
        if let Some(bit) = self.resume_bit.take() {
            self.bits.skip_bits_of_next_byte(bit)?;
        } else {
            self.take_checkpoint();
        }
        self.final_block = self.bits.take(1)? == 1;
        match self.bits.take(2)? {
            0 => {
                self.bits.align();
                let len = self.bits.take(16)?;
                let nlen = self.bits.take(16)?;
                if len != !nlen & 0xffff {
                    return Err(invalid("stored block length mismatch"));
                }
                self.state = State::Stored {
                    remaining: u16::try_from(len).map_err(|_| invalid("stored block too long"))?,
                };
            }
            1 => {
                self.block = Some(Block {
                    literals: Huffman::fixed_literals()?,
                    distances: Huffman::fixed_distances()?,
                });
                self.state = State::Huffman;
            }
            2 => {
                self.read_dynamic_tables()?;
                self.state = State::Huffman;
            }
            _ => return Err(invalid("reserved block type")),
        }
        Ok(())
    }

    fn read_dynamic_tables(&mut self) -> io::Result<()> {
        let hlit = self.bits.take(5)? as usize + 257;
        let hdist = self.bits.take(5)? as usize + 1;
        let hclen = self.bits.take(4)? as usize + 4;
        let mut code_lengths = [0u8; 19];
        for &index in &CODE_LENGTH_ORDER[..hclen] {
            code_lengths[index] = u8::try_from(self.bits.take(3)?).unwrap_or(u8::MAX);
        }
        let code_length_code = Huffman::new(&code_lengths)?;
        let mut lengths = vec![0u8; hlit + hdist];
        let mut i = 0;
        while i < lengths.len() {
            let symbol = self.bits.decode(&code_length_code)?;
            match symbol {
                0..=15 => {
                    lengths[i] = u8::try_from(symbol).unwrap_or(u8::MAX);
                    i += 1;
                }
                16 => {
                    if i == 0 {
                        return Err(invalid("repeat with no previous length"));
                    }
                    let previous = lengths[i - 1];
                    let repeat = 3 + self.bits.take(2)? as usize;
                    if i + repeat > lengths.len() {
                        return Err(invalid("code lengths overrun"));
                    }
                    lengths[i..i + repeat].fill(previous);
                    i += repeat;
                }
                17 | 18 => {
                    let repeat = if symbol == 17 {
                        3 + self.bits.take(3)? as usize
                    } else {
                        11 + self.bits.take(7)? as usize
                    };
                    if i + repeat > lengths.len() {
                        return Err(invalid("code lengths overrun"));
                    }
                    i += repeat;
                }
                _ => return Err(invalid("bad code length symbol")),
            }
        }
        if lengths[256] == 0 {
            return Err(invalid("block has no end-of-block code"));
        }
        self.block = Some(Block {
            literals: Huffman::new(&lengths[..hlit])?,
            distances: Huffman::new(&lengths[hlit..])?,
        });
        Ok(())
    }

    fn emit(&mut self, byte: u8) {
        self.ring[slot(self.pos)] = byte;
        self.pos += 1;
    }

    /// Decodes the block's symbols until it ends or [`WINDOW`] bytes wait to
    /// be read. The hot loop: the position and the codes live in locals, and
    /// nothing goes back through the state machine per symbol.
    fn run_block(&mut self, block: &Block) -> io::Result<()> {
        self.run_fast(block)?;
        if self.state == State::Huffman {
            self.run_checked(block)?;
        }
        Ok(())
    }

    /// The fast loop: with room for the longest match and a chunk of input
    /// ahead, one symbol decodes with no end-of-input or room check at all.
    /// The reader's cursor and bit buffer live in locals meanwhile, written
    /// back around the rare slow path and at the end, so they stay in
    /// registers. Leaves the block in [`State::Huffman`] for the checked
    /// loop when the window or the input runs short.
    fn run_fast(&mut self, block: &Block) -> io::Result<()> {
        let limit = self.served + WINDOW as u64;
        let mut pos = self.pos;
        let ring = &mut self.ring;
        let bits = &mut self.bits;
        let mask = FAST_SIZE as u64 - 1;
        bits.top_up()?;
        let mut b = bits.bits;
        let mut n = bits.nbits;
        let mut p = bits.pos;
        while pos + FAST_OUTPUT <= limit {
            // The chunk runs low long before the input does: slide it and
            // read on, leaving only a true end of input to the checked loop.
            if bits.len - p < FAST_INPUT {
                p = bits.top_up_from(p)?;
                if bits.len - p < FAST_INPUT {
                    break;
                }
            }
            fill32!(bits, p, b, n);
            let entry = block.literals.fast[usize::try_from(b & mask).unwrap_or(0) % FAST_SIZE];
            let symbol = if entry.trailing_zeros() >= 4 {
                slow!(bits, &block.literals, p, b, n)
            } else {
                b >>= entry & 0xf;
                n -= u32::from(entry & 0xf);
                entry >> 4
            };
            if let Ok(byte) = u8::try_from(symbol) {
                ring[slot(pos)] = byte;
                pos += 1;
                continue;
            }
            if symbol == 256 {
                self.state = State::BlockEnd;
                break;
            }
            let index = usize::from(symbol - 257);
            let base = *LENGTH_BASE
                .get(index)
                .ok_or_else(|| invalid("bad length"))?;
            fill32!(bits, p, b, n);
            let extra = u32::from(LENGTH_EXTRA[index]);
            let mut length = u64::from(base) + (b & ((1u64 << extra) - 1));
            b >>= extra;
            n -= extra;
            fill32!(bits, p, b, n);
            let entry = block.distances.fast[usize::try_from(b & mask).unwrap_or(0) % FAST_SIZE];
            let dsym = if entry.trailing_zeros() >= 4 {
                slow!(bits, &block.distances, p, b, n)
            } else {
                b >>= entry & 0xf;
                n -= u32::from(entry & 0xf);
                entry >> 4
            };
            let dsym = usize::from(dsym);
            let dbase = *DIST_BASE.get(dsym).ok_or_else(|| invalid("bad distance"))?;
            fill32!(bits, p, b, n);
            let extra = u32::from(DIST_EXTRA[dsym]);
            let distance = u64::from(dbase) + (b & ((1u64 << extra) - 1));
            b >>= extra;
            n -= extra;
            if distance > pos || distance > WINDOW as u64 {
                return Err(invalid("distance too far back"));
            }
            // Sixteen bytes at a time for short copies, over-copying inside
            // the ring, since a call per match costs more than the bytes; a
            // run (a reference into what it writes) repeats in chunks of its
            // distance.
            while length > 0 {
                let chunk = length.min(distance);
                let (src, dst) = (slot(pos - distance), slot(pos));
                let len = usize::try_from(chunk).unwrap_or(0);
                if chunk <= 16 && src + 16 <= RING && dst + 16 <= RING {
                    let word: [u8; 16] = ring[src..src + 16].try_into().unwrap_or([0; 16]);
                    ring[dst..dst + 16].copy_from_slice(&word);
                    pos += chunk;
                    length -= chunk;
                } else if src + len <= RING && dst + len <= RING {
                    ring.copy_within(src..src + len, dst);
                    pos += chunk;
                    length -= chunk;
                } else {
                    ring[dst] = ring[src];
                    pos += 1;
                    length -= 1;
                }
            }
        }
        bits.consumed += (p - bits.pos) as u64;
        bits.pos = p;
        bits.bits = b;
        bits.nbits = n;
        self.pos = pos;
        Ok(())
    }

    /// The checked loop, for the last bytes before the window cap or the end
    /// of input: every read may find the input short.
    fn run_checked(&mut self, block: &Block) -> io::Result<()> {
        let limit = self.served + WINDOW as u64;
        let mut pos = self.pos;
        let ring = &mut self.ring;
        let bits = &mut self.bits;
        while pos < limit {
            let symbol = bits.decode(&block.literals)?;
            if let Ok(byte) = u8::try_from(symbol) {
                ring[slot(pos)] = byte;
                pos += 1;
                continue;
            }
            if symbol == 256 {
                self.state = State::BlockEnd;
                break;
            }
            let index = usize::from(symbol - 257);
            let base = *LENGTH_BASE
                .get(index)
                .ok_or_else(|| invalid("bad length"))?;
            let mut length =
                u64::from(base) + u64::from(bits.take(u32::from(LENGTH_EXTRA[index]))?);
            let dsym = usize::from(bits.decode(&block.distances)?);
            let dbase = *DIST_BASE.get(dsym).ok_or_else(|| invalid("bad distance"))?;
            let distance = u64::from(dbase) + u64::from(bits.take(u32::from(DIST_EXTRA[dsym]))?);
            if distance > pos || distance > WINDOW as u64 {
                return Err(invalid("distance too far back"));
            }
            // Slice copies of at most `distance` bytes, so a run (a reference
            // into what it writes) repeats correctly; a copy that would wrap
            // the ring goes by byte.
            while length > 0 {
                let chunk = length.min(distance);
                let (src, dst) = (slot(pos - distance), slot(pos));
                let len = usize::try_from(chunk).unwrap_or(0);
                if chunk <= 16 && src + 16 <= RING && dst + 16 <= RING {
                    // Sixteen bytes always: over-copying inside the ring is
                    // harmless and cheaper than a length-dependent loop.
                    let word: [u8; 16] = ring[src..src + 16].try_into().unwrap_or([0; 16]);
                    ring[dst..dst + 16].copy_from_slice(&word);
                    pos += chunk;
                    length -= chunk;
                } else if src + len <= RING && dst + len <= RING {
                    ring.copy_within(src..src + len, dst);
                    pos += chunk;
                    length -= chunk;
                } else {
                    ring[dst] = ring[src];
                    pos += 1;
                    length -= 1;
                }
            }
        }
        self.pos = pos;
        Ok(())
    }

    /// Decodes until [`WINDOW`] bytes wait to be read, a block ends or the
    /// stream does; the cap keeps the ring's history intact behind `served`.
    fn fill(&mut self) -> io::Result<()> {
        while self.pos - self.served < WINDOW as u64 {
            match self.state {
                State::MemberStart => {
                    self.read_member_header()?;
                    self.state = State::BlockStart;
                }
                State::BlockStart => self.start_block()?,
                State::Stored { remaining } => {
                    if remaining == 0 {
                        self.state = State::BlockEnd;
                    } else {
                        let byte = self.bits.next_byte()?;
                        self.emit(byte);
                        self.state = State::Stored {
                            remaining: remaining - 1,
                        };
                    }
                }
                State::Huffman => {
                    let block = self
                        .block
                        .take()
                        .ok_or_else(|| invalid("no code table for this block"))?;
                    let result = self.run_block(&block);
                    self.block = Some(block);
                    result?;
                }
                State::BlockEnd => {
                    self.state = if self.final_block {
                        State::MemberEnd
                    } else {
                        State::BlockStart
                    };
                    // A block boundary is the only place to stop cleanly.
                    return Ok(());
                }
                State::MemberEnd => {
                    self.bits.align();
                    self.bits.take(32)?; // CRC-32, not verified: the blob's digest already was
                    self.bits.take(32)?; // ISIZE
                    match self.bits.next_byte_or_end()? {
                        None => self.state = State::Done,
                        Some(id1) => {
                            let id2 = self.bits.next_byte()?;
                            self.read_member_header_after_magic(id1, id2)?;
                            self.state = State::BlockStart;
                        }
                    }
                }
                State::Done => return Ok(()),
            }
        }
        Ok(())
    }
}

impl<R: Read> Read for Inflater<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.pos == self.served {
            if self.state == State::Done {
                return Ok(0);
            }
            self.fill()?;
        }
        let start = slot(self.served);
        let unread = usize::try_from(self.pos - self.served).unwrap_or(usize::MAX);
        let n = unread.min(buf.len()).min(RING - start);
        buf[..n].copy_from_slice(&self.ring[start..start + n]);
        self.served += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use flate2::{Compression, write::GzEncoder};

    use super::{Checkpoint, Inflater, WINDOW};

    fn gzip(data: &[u8], level: u32) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    /// Text-like data: repeats with drift, so every block type and distance
    /// gets exercised.
    fn sample(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|i| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                if i % 7 == 0 {
                    u8::try_from(state % 26).unwrap() + b'a'
                } else {
                    b"the quick brown fox "[i % 20]
                }
            })
            .collect()
    }

    fn inflate_all(compressed: &[u8], every: u64) -> (Vec<u8>, Vec<Checkpoint>) {
        let mut inflater = Inflater::new(Cursor::new(compressed), every).unwrap();
        let mut out = Vec::new();
        inflater.read_to_end(&mut out).unwrap();
        (out, inflater.into_checkpoints())
    }

    #[test]
    fn inflates_every_compression_level() {
        let data = sample(200_000);
        for level in [0, 1, 6, 9] {
            let (out, _) = inflate_all(&gzip(&data, level), u64::MAX);
            assert_eq!(out, data, "level {level}");
        }
    }

    #[test]
    fn inflates_a_stream_of_several_members() {
        let a = sample(50_000);
        let b = sample(70_000);
        let mut stream = gzip(&a, 6);
        stream.extend(gzip(&b, 1));
        let (out, _) = inflate_all(&stream, u64::MAX);
        assert_eq!(out, [a, b].concat());
    }

    #[test]
    fn checkpoints_resume_to_the_same_output() {
        let data = sample(1_500_000);
        let compressed = gzip(&data, 6);
        let (out, checkpoints) = inflate_all(&compressed, 128 * 1024);
        assert_eq!(out, data);
        assert!(
            checkpoints.len() >= 5,
            "expected a checkpoint every few blocks, got {}",
            checkpoints.len()
        );
        for checkpoint in &checkpoints {
            assert!(checkpoint.window.len() <= WINDOW);
            let input = Cursor::new(&compressed[usize::try_from(checkpoint.in_offset).unwrap()..]);
            let mut resumed = Inflater::resume(input, checkpoint).unwrap();
            let mut rest = Vec::new();
            resumed.read_to_end(&mut rest).unwrap();
            assert_eq!(
                rest,
                &data[usize::try_from(checkpoint.out_offset).unwrap()..],
                "resume at {}",
                checkpoint.out_offset
            );
        }
    }

    #[test]
    fn skip_lands_on_the_offset() {
        let data = sample(300_000);
        let mut inflater = Inflater::new(Cursor::new(gzip(&data, 6)), u64::MAX).unwrap();
        inflater.skip(123_456).unwrap();
        let mut chunk = vec![0u8; 1000];
        inflater.read_exact(&mut chunk).unwrap();
        assert_eq!(chunk, &data[123_456..124_456]);
    }

    #[test]
    fn a_truncated_stream_is_an_error_not_a_hang() {
        let compressed = gzip(&sample(100_000), 6);
        let mut inflater =
            Inflater::new(Cursor::new(&compressed[..compressed.len() / 2]), u64::MAX).unwrap();
        let mut out = Vec::new();
        assert!(inflater.read_to_end(&mut out).is_err());
    }
}
