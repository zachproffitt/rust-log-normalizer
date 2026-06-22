//! Windows Event Log (NDJSON) → [`NormalizedEvent`].
//!
//! Input is one Windows event per line as JSON (see `test/data/json`). The data
//! is deeply nested and only partially present per event, so we navigate a
//! `serde_json::Value` rather than deriving rigid structs.

use anyhow::Context;
use chrono::{DateTime, Utc};
use serde_json::Value;

use super::{clean_optional, EventCategory, EventOutcome, EventType, NormalizedEvent};

pub fn parse(raw: &str) -> anyhow::Result<NormalizedEvent> {
    let root: Value = serde_json::from_str(raw).context("invalid Windows Event Log JSON")?;

    let event_id = root
        .pointer("/System/EventID")
        .and_then(Value::as_i64)
        .context("missing System.EventID")?;

    Ok(NormalizedEvent {
        timestamp: timestamp(&root),
        event_type: event_type(event_id),
        event_category: event_category(event_id),
        event_outcome: outcome(&root),
        source_ip: source_ip(&root),
        user_name: user_name(&root),
        host_name: str_at(&root, "/System/Computer").and_then(|s| clean_optional(&s)),
        log_level: log_level(&root),
        message: message(&root),
    })
}

/// Reads `System.TimeCreated` (RFC 3339) and renders it as UTC millisecond ISO 8601.
/// Falls back to the current time if absent or unparseable.
fn timestamp(root: &Value) -> String {
    str_at(root, "/System/TimeCreated")
        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn event_type(event_id: i64) -> EventType {
    match event_id {
        4624 | 4648 | 4625 | 4688 => EventType::Start,
        4634 | 4647 | 4689 => EventType::End,
        4720..=4767 => EventType::Info,
        _ => EventType::Info,
    }
}

fn event_category(event_id: i64) -> EventCategory {
    match event_id {
        4624 | 4625 | 4634 | 4647 | 4648 => EventCategory::Authentication,
        4688 | 4689 => EventCategory::Process,
        4720..=4767 => EventCategory::Host,
        _ => EventCategory::Host,
    }
}

/// Derives outcome from `RenderingInfo.Keywords`.
fn outcome(root: &Value) -> EventOutcome {
    let keywords = root.pointer("/RenderingInfo/Keywords");
    let contains = |needle: &str| {
        keywords
            .and_then(Value::as_array)
            .map(|arr| arr.iter().filter_map(Value::as_str).any(|k| k == needle))
            .unwrap_or(false)
    };

    if contains("Audit Success") {
        EventOutcome::Success
    } else if contains("Audit Failure") {
        EventOutcome::Failure
    } else {
        EventOutcome::Unknown
    }
}

/// Prefers `EventData.IpAddress`, falling back to `OpenWEC.IpAddress`.
fn source_ip(root: &Value) -> Option<String> {
    str_at(root, "/EventData/IpAddress")
        .and_then(|s| clean_optional(&s))
        .or_else(|| str_at(root, "/OpenWEC/IpAddress").and_then(|s| clean_optional(&s)))
}

/// Prefers `EventData.TargetUserName`, falling back to `EventData.SubjectUserName`.
fn user_name(root: &Value) -> Option<String> {
    str_at(root, "/EventData/TargetUserName")
        .and_then(|s| clean_optional(&s))
        .or_else(|| str_at(root, "/EventData/SubjectUserName").and_then(|s| clean_optional(&s)))
}

/// Normalizes `RenderingInfo.Level` (`Information` → `info`, else lowercased).
fn log_level(root: &Value) -> Option<String> {
    str_at(root, "/RenderingInfo/Level").and_then(|level| {
        clean_optional(&level).map(|l| match l.as_str() {
            "Information" => "info".to_string(),
            other => other.to_lowercase(),
        })
    })
}

/// Uses `RenderingInfo.Message`, falling back to the provider name then a default.
fn message(root: &Value) -> String {
    str_at(root, "/RenderingInfo/Message")
        .and_then(|s| clean_optional(&s))
        .or_else(|| str_at(root, "/System/Provider/Name").and_then(|s| clean_optional(&s)))
        .unwrap_or_else(|| "Windows event".to_string())
}

/// Reads a JSON-pointer location as an owned `String` when it is a string value.
fn str_at(root: &Value, pointer: &str) -> Option<String> {
    root.pointer(pointer)
        .and_then(Value::as_str)
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_sample(json: &str) -> NormalizedEvent {
        parse(json).expect("sample should parse")
    }

    #[test]
    fn sample_2_matches_spec_example() {
        // Mirrors the spec's "Example Output Record".
        let event = parse_sample(include_str!("../../test/data/json/sample-2.json"));
        assert_eq!(
            event,
            NormalizedEvent {
                timestamp: "2026-02-14T15:45:33.221Z".to_string(),
                event_type: EventType::Start,
                event_category: EventCategory::Authentication,
                event_outcome: EventOutcome::Failure,
                source_ip: Some("10.99.0.55".to_string()),
                user_name: Some("admin".to_string()),
                host_name: Some("dc01.contoso.local".to_string()),
                log_level: Some("info".to_string()),
                message: "An account failed to log on.".to_string(),
            }
        );
    }

    #[test]
    fn sample_1_successful_logon() {
        let event = parse_sample(include_str!("../../test/data/json/sample-1.json"));
        assert_eq!(event.event_type, EventType::Start);
        assert_eq!(event.event_category, EventCategory::Authentication);
        assert_eq!(event.event_outcome, EventOutcome::Success);
        assert_eq!(event.source_ip.as_deref(), Some("10.0.50.42"));
        assert_eq!(event.user_name.as_deref(), Some("jsmith"));
        assert_eq!(event.timestamp, "2026-02-14T14:22:10.883Z");
    }

    #[test]
    fn sample_3_process_creation_uses_fallbacks() {
        // 4688: process category; TargetUserName is "-" so falls back to Subject;
        // EventData has no IpAddress so source.ip falls back to OpenWEC.
        let event = parse_sample(include_str!("../../test/data/json/sample-3.json"));
        assert_eq!(event.event_type, EventType::Start);
        assert_eq!(event.event_category, EventCategory::Process);
        assert_eq!(event.event_outcome, EventOutcome::Success);
        assert_eq!(event.user_name.as_deref(), Some("DC01$"));
        assert_eq!(event.source_ip.as_deref(), Some("192.168.1.10"));
    }

    #[test]
    fn sentinel_and_missing_fields_are_omitted() {
        let json = r#"{
            "System": {"EventID": 9999, "Computer": "host1",
                       "TimeCreated": "2026-01-01T00:00:00.0000000Z"},
            "EventData": {"TargetUserName": "-", "SubjectUserName": "-", "IpAddress": ""},
            "RenderingInfo": {"Message": "x"}
        }"#;
        let event = parse_sample(json);
        assert_eq!(event.event_type, EventType::Info);
        assert_eq!(event.event_category, EventCategory::Host);
        assert_eq!(event.event_outcome, EventOutcome::Unknown);
        assert_eq!(event.source_ip, None);
        assert_eq!(event.user_name, None);
        assert_eq!(event.log_level, None);
    }

    #[test]
    fn iam_event_range_is_info_host() {
        assert_eq!(event_type(4720), EventType::Info);
        assert_eq!(event_category(4720), EventCategory::Host);
        assert_eq!(event_type(4767), EventType::Info);
        assert_eq!(event_category(4767), EventCategory::Host);
    }
}
