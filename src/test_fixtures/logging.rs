//! Tracing capture for tests that assert on emitted log lines.

use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};

use tracing_subscriber::fmt::MakeWriter;

/// Collects tracing output into a shared buffer; every clone appends to the
/// same buffer. Pass a clone to `with_writer` and read back via `contents`.
#[derive(Clone, Default)]
pub struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl LogCapture {
    /// Returns everything captured so far.
    ///
    /// # Panics
    ///
    /// Panics if the captured bytes are not valid UTF-8.
    pub fn contents(&self) -> String {
        let bytes = self.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }
}

impl Write for LogCapture {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
