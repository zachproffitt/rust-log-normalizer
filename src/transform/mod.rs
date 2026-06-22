//! Transform seam: turns a raw event line into a [`NormalizedEvent`].
//!
//! Both input formats (Windows Event Log NDJSON and RFC 3164 syslog wrapping CEF)
//! are mapped into one common schema with flat, dotted top-level keys (e.g.
//! `event.type`). The per-format mapping lives in the [`windows`] and [`syslog`]
//! submodules; [`transform`] dispatches to them based on the detected [`Format`].
//!
//! Serialization is left to the caller ([`crate::pipeline`]) so the result is
//! testable as data rather than as side effects.

use serde::Serialize;

use crate::format::Format;

mod syslog;
mod windows;

/// The common normalized output record.
///
/// Field order matches the spec's example output. Optional fields are omitted
/// from the JSON when absent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NormalizedEvent {
    #[serde(rename = "@timestamp")]
    pub timestamp: String,
    #[serde(rename = "event.type")]
    pub event_type: EventType,
    #[serde(rename = "event.category")]
    pub event_category: EventCategory,
    #[serde(rename = "event.outcome")]
    pub event_outcome: EventOutcome,
    #[serde(rename = "source.ip", skip_serializing_if = "Option::is_none")]
    pub source_ip: Option<String>,
    #[serde(rename = "user.name", skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(rename = "host.name", skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(rename = "log.level", skip_serializing_if = "Option::is_none")]
    pub log_level: Option<String>,
    #[serde(rename = "message")]
    pub message: String,
}

/// `event.type`: one of `start`, `end`, `info`, `denied`, `allowed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventType {
    Start,
    End,
    Info,
    Denied,
    Allowed,
}

/// `event.category`: one of `authentication`, `network`, `process`, `host`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventCategory {
    Authentication,
    Network,
    Process,
    Host,
}

/// `event.outcome`: one of `success`, `failure`, `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventOutcome {
    Success,
    Failure,
    Unknown,
}

/// Maps a single raw event line into a [`NormalizedEvent`] using the parser for
/// its detected `format`.
pub fn transform(format: Format, raw: &str) -> anyhow::Result<NormalizedEvent> {
    match format {
        Format::Syslog => syslog::parse(raw),
        Format::Ndjson => windows::parse(raw),
    }
}

/// Normalizes an optional string field: trims it and returns `None` for empty
/// values or the `-` sentinel that Windows Event Log uses for "not applicable".
pub(crate) fn clean_optional(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "-" {
        None
    } else {
        Some(trimmed.to_string())
    }
}
