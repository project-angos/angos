//! What `file` would say of an ELF binary, and the libraries it needs, read
//! from as few of its bytes as that takes: the dynamic section of a large
//! binary lies tens of megabytes in.

use std::future::Future;

use angos_extension_service::ElfDetails;

/// The header, and in practice the program headers, loader and notes after it.
pub const HEAD_LEN: u64 = 64 * 1024;
/// A segment read's ceiling, past any real loader, note or dynamic section, so
/// a forged header cannot ask for a whole file.
const SEGMENT_LIMIT: u64 = 1024 * 1024;
/// What is read of each library name, longer than any real one.
const NAME_LIMIT: u64 = 256;
const MAGIC: &[u8] = b"\x7fELF";
const PT_LOAD: u32 = 1;
const PT_DYNAMIC: u32 = 2;
const PT_INTERP: u32 = 3;
const PT_NOTE: u32 = 4;
const PT_GNU_STACK: u32 = 0x6474_e551;
const PT_GNU_RELRO: u32 = 0x6474_e552;
const PF_X: u32 = 1;
const NT_GNU_BUILD_ID: u32 = 3;
const DT_NEEDED: u64 = 1;
const DT_STRTAB: u64 = 5;
const DT_STRSZ: u64 = 10;
const DT_SONAME: u64 = 14;
const DT_FLAGS: u64 = 30;
const DT_FLAGS_1: u64 = 0x6fff_fffb;
const DF_BIND_NOW: u64 = 8;
const DF_1_NOW: u64 = 1;

/// A binary's integer width and byte order.
#[derive(Clone, Copy)]
struct Layout {
    wide: bool,
    little: bool,
}

impl Layout {
    fn u16(self, bytes: &[u8], at: usize) -> Option<u16> {
        let raw = bytes.get(at..at.checked_add(2)?)?.try_into().ok()?;
        Some(if self.little {
            u16::from_le_bytes(raw)
        } else {
            u16::from_be_bytes(raw)
        })
    }

    fn u32(self, bytes: &[u8], at: usize) -> Option<u32> {
        let raw = bytes.get(at..at.checked_add(4)?)?.try_into().ok()?;
        Some(if self.little {
            u32::from_le_bytes(raw)
        } else {
            u32::from_be_bytes(raw)
        })
    }

    fn u64(self, bytes: &[u8], at: usize) -> Option<u64> {
        let raw = bytes.get(at..at.checked_add(8)?)?.try_into().ok()?;
        Some(if self.little {
            u64::from_le_bytes(raw)
        } else {
            u64::from_be_bytes(raw)
        })
    }

    /// An address, offset or size: 8 bytes in a 64-bit binary, 4 in a 32-bit one.
    fn word(self, bytes: &[u8], at: usize) -> Option<u64> {
        if self.wide {
            self.u64(bytes, at)
        } else {
            self.u32(bytes, at).map(u64::from)
        }
    }
}

struct Segment {
    kind: u32,
    flags: u32,
    offset: u64,
    address: u64,
    size: u64,
}

/// The binary whose first bytes are `head`, reading the rest through
/// `read(offset, length)`; `None` when it is no ELF or too mangled to read.
///
/// # Errors
/// Fails when `read` does.
pub async fn describe<F, Fut, E>(head: &[u8], read: F) -> Result<Option<ElfDetails>, E>
where
    F: Fn(u64, u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, E>>,
{
    if !head.starts_with(MAGIC) {
        return Ok(None);
    }
    let layout = Layout {
        wide: head.get(4) == Some(&2),
        little: head.get(5) == Some(&1),
    };
    let wide = layout.wide;
    let header = (|| {
        Some((
            layout.u16(head, 16)?,
            layout.u16(head, 18)?,
            layout.word(head, 24)?,
            layout.word(head, if wide { 32 } else { 28 })?,
            layout.u16(head, if wide { 54 } else { 42 })?,
            layout.u16(head, if wide { 56 } else { 44 })?,
        ))
    })();
    let Some((kind, machine, entry, phoff, entry_size, count)) = header else {
        return Ok(None);
    };
    let length = u64::from(entry_size) * u64::from(count);
    let table = bytes_at(head, &read, phoff, length).await?;
    let segments: Vec<Segment> = (0..usize::from(count))
        .map_while(|index| {
            let at = index * usize::from(entry_size);
            Some(Segment {
                kind: layout.u32(&table, at)?,
                flags: layout.u32(&table, at + if wide { 4 } else { 24 })?,
                offset: layout.word(&table, at + if wide { 8 } else { 4 })?,
                address: layout.word(&table, at + if wide { 16 } else { 8 })?,
                size: layout.word(&table, at + if wide { 32 } else { 16 })?,
            })
        })
        .collect();

    let mut interpreter = None;
    let mut build_id = None;
    let mut linking = Linking::default();
    for segment in &segments {
        match segment.kind {
            PT_INTERP => {
                let bytes = bytes_at(head, &read, segment.offset, segment.size).await?;
                interpreter = Some(name(&bytes, 0));
            }
            PT_NOTE if build_id.is_none() => {
                let bytes = bytes_at(head, &read, segment.offset, segment.size).await?;
                build_id = gnu_build_id(layout, &bytes);
            }
            PT_DYNAMIC => {
                let bytes = bytes_at(head, &read, segment.offset, segment.size).await?;
                linking = read_dynamic(layout, head, &bytes, &segments, &read).await?;
            }
            _ => {}
        }
    }
    let has = |kind| segments.iter().any(|segment| segment.kind == kind);
    let relro = match (has(PT_GNU_RELRO), linking.bind_now) {
        (false, _) => "none",
        (true, false) => "partial",
        (true, true) => "full",
    };
    // Without a PT_GNU_STACK, the loader makes the stack executable.
    let executable_stack = segments
        .iter()
        .find(|segment| segment.kind == PT_GNU_STACK)
        .is_none_or(|segment| segment.flags & PF_X != 0);
    Ok(Some(ElfDetails {
        // A shared object naming a loader is a position-independent executable.
        kind: match kind {
            1 => "Relocatable object".to_string(),
            2 => "Executable".to_string(),
            3 if interpreter.is_some() => "PIE executable".to_string(),
            3 => "Shared object".to_string(),
            4 => "Core dump".to_string(),
            other => format!("Type {other}"),
        },
        machine: match machine {
            3 => "x86".to_string(),
            8 => "MIPS".to_string(),
            20 => "PowerPC".to_string(),
            21 => "PowerPC64".to_string(),
            22 => "s390x".to_string(),
            40 => "ARM".to_string(),
            62 => "x86-64".to_string(),
            183 => "AArch64".to_string(),
            243 => "RISC-V".to_string(),
            258 => "LoongArch".to_string(),
            other => format!("Machine {other}"),
        },
        bits: if wide { 64 } else { 32 },
        endian: if layout.little { "little" } else { "big" }.to_string(),
        entry: format!("{entry:#x}"),
        interpreter,
        dynamic: has(PT_DYNAMIC),
        needed: linking.needed,
        soname: linking.soname,
        build_id,
        relro: relro.to_string(),
        executable_stack,
    }))
}

/// `length` bytes from `offset`, out of `head` when it holds them.
async fn bytes_at<F, Fut, E>(head: &[u8], read: &F, offset: u64, length: u64) -> Result<Vec<u8>, E>
where
    F: Fn(u64, u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, E>>,
{
    let length = length.min(SEGMENT_LIMIT);
    let within = usize::try_from(offset)
        .ok()
        .zip(usize::try_from(offset.saturating_add(length)).ok())
        .and_then(|(from, to)| head.get(from..to));
    match within {
        Some(bytes) => Ok(bytes.to_vec()),
        None => read(offset, length).await,
    }
}

/// The string at `at`, up to its NUL.
fn name(bytes: &[u8], at: usize) -> String {
    let rest = bytes.get(at..).unwrap_or_default();
    let end = rest
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(rest.len());
    String::from_utf8_lossy(&rest[..end]).into_owned()
}

/// The GNU build ID among notes, each a name and a description padded to 4 bytes.
fn gnu_build_id(layout: Layout, notes: &[u8]) -> Option<String> {
    let pad = |size: u32| usize::try_from(size).ok().map(|size| size.div_ceil(4) * 4);
    let mut at = 0;
    while at + 12 <= notes.len() {
        let (name_size, desc_size) = (layout.u32(notes, at)?, layout.u32(notes, at + 4)?);
        let desc = (at + 12).checked_add(pad(name_size)?)?;
        if layout.u32(notes, at + 8)? == NT_GNU_BUILD_ID
            && notes.get(at + 12..at + 15) == Some(b"GNU".as_slice())
        {
            let id = notes.get(desc..desc.checked_add(usize::try_from(desc_size).ok()?)?)?;
            return Some(hex::encode(id));
        }
        at = desc.checked_add(pad(desc_size)?)?;
    }
    None
}

/// What the dynamic section says: the libraries it needs, the name a library
/// answers to, and whether every symbol binds at load, which makes RELRO full.
#[derive(Default)]
struct Linking {
    needed: Vec<String>,
    soname: Option<String>,
    bind_now: bool,
}

/// The dynamic section's [`Linking`], its names read out of the string table
/// it points at.
async fn read_dynamic<F, Fut, E>(
    layout: Layout,
    head: &[u8],
    dynamic: &[u8],
    segments: &[Segment],
    read: &F,
) -> Result<Linking, E>
where
    F: Fn(u64, u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, E>>,
{
    let size = if layout.wide { 16 } else { 8 };
    let (mut libraries, mut soname, mut table, mut table_size) = (Vec::new(), None, 0, 0);
    let mut bind_now = false;
    for at in (0..dynamic.len() / size).map(|index| index * size) {
        let (Some(tag), Some(value)) = (
            layout.word(dynamic, at),
            layout.word(dynamic, at + size / 2),
        ) else {
            break;
        };
        match tag {
            0 => break,
            DT_NEEDED => libraries.push(value),
            DT_SONAME => soname = Some(value),
            DT_STRTAB => table = value,
            DT_STRSZ => table_size = value,
            DT_FLAGS => bind_now |= value & DF_BIND_NOW != 0,
            DT_FLAGS_1 => bind_now |= value & DF_1_NOW != 0,
            _ => {}
        }
    }
    // The table is named by its address: the segment loading it places it in the file.
    let place = segments.iter().find(|segment| {
        segment.kind == PT_LOAD
            && segment.address <= table
            && table < segment.address.saturating_add(segment.size)
    });
    let unnamed = Linking {
        bind_now,
        ..Linking::default()
    };
    let Some(place) = place else {
        return Ok(unnamed);
    };
    let offsets = libraries.iter().chain(soname.iter());
    let (Some(&first), Some(&last)) = (offsets.clone().min(), offsets.max()) else {
        return Ok(unnamed);
    };
    // Only the stretch holding the names: a large binary's table runs to megabytes.
    let start = place
        .offset
        .saturating_add(table - place.address)
        .saturating_add(first);
    let length = last
        .saturating_add(NAME_LIMIT)
        .min(table_size)
        .saturating_sub(first);
    let window = bytes_at(head, read, start, length).await?;
    let at = |offset: u64| {
        name(
            &window,
            usize::try_from(offset - first).unwrap_or(usize::MAX),
        )
    };
    Ok(Linking {
        needed: libraries.iter().map(|&offset| at(offset)).collect(),
        soname: soname.map(at),
        bind_now,
    })
}
