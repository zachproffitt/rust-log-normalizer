//! Transform seam: turns a raw event line into normalized output.
//!
//! This is currently a **stub**. It preserves the original echo behaviour by
//! printing each event tagged with its detected format. The real RFC 3164 / CEF
//! and NDJSON parsing and the common normalized output schema land in the next
//! increment; they will slot in behind this same entry point.
//!
//! [`transform`] returns a `Result` so that future parse/normalize failures flow
//! into the pipeline's log-and-drop handling without changing the call site.

use crate::format::Format;

/// Transforms a single raw event line according to its `format`.
///
/// Stub behaviour: echoes the raw line to stdout, tagged with the format.
pub fn transform(format: Format, raw: &str) -> anyhow::Result<()> {
    match format {
        Format::Syslog => syslog(raw),
        Format::Ndjson => ndjson(raw),
    }
}

/// Stub RFC 3164 syslog transform.
fn syslog(raw: &str) -> anyhow::Result<()> {
    println!("[syslog] {raw}");
    Ok(())
}

/// Stub NDJSON transform.
fn ndjson(raw: &str) -> anyhow::Result<()> {
    println!("[ndjson] {raw}");
    Ok(())
}
