//! Wire-format detection for a connection.
//!
//! Both supported formats are line-delimited, so framing is identical; detection
//! only decides which transform each line is routed to. Detection is locked once
//! per connection (see [`crate::pipeline`]).

/// A supported input format on a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// RFC 3164 syslog, e.g. `<134>Dec 05 10:30:45 host ...`.
    Syslog,
    /// Newline-delimited JSON: one compact JSON object per line.
    Ndjson,
}

/// Detects the format of a single line from its first non-whitespace byte.
///
/// Returns `None` when the line is empty/whitespace-only or starts with a byte
/// that matches neither format.
pub fn detect(line: &str) -> Option<Format> {
    match line.trim_start().as_bytes().first()? {
        b'<' => Some(Format::Syslog),
        b'{' => Some(Format::Ndjson),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_syslog_from_pri() {
        let line = "<134>Dec 05 10:30:45 192.168.1.1 CEF:0|Microsoft|Windows|10.0|4624";
        assert_eq!(detect(line), Some(Format::Syslog));
    }

    #[test]
    fn detects_ndjson_object() {
        assert_eq!(detect(r#"{"EventID":4624}"#), Some(Format::Ndjson));
    }

    #[test]
    fn ignores_leading_whitespace() {
        assert_eq!(detect("   \t{\"a\":1}"), Some(Format::Ndjson));
        assert_eq!(detect("  <13>msg"), Some(Format::Syslog));
    }

    #[test]
    fn unknown_first_byte_is_none() {
        assert_eq!(detect("plain text line"), None);
        assert_eq!(detect("[133]not-syslog"), None);
    }

    #[test]
    fn empty_or_whitespace_is_none() {
        assert_eq!(detect(""), None);
        assert_eq!(detect("   \t  "), None);
    }
}
