//! RFC 3164 syslog (wrapping CEF) → [`NormalizedEvent`].
//!
//! Wire shape: `<PRI>Mmm dd HH:MM:SS HOST CEF:0|vendor|product|ver|sigid|name|sev|ext`
//! where `ext` is space-separated `key=value` pairs (values may contain spaces).

use std::collections::HashMap;

use chrono::{Datelike, NaiveDateTime, TimeZone, Utc};

use super::{clean_optional, EventCategory, EventOutcome, EventType, NormalizedEvent};

pub fn parse(raw: &str) -> anyhow::Result<NormalizedEvent> {
    let (pri, after_pri) = strip_pri(raw);

    // Split the line at the CEF marker: before it is the RFC 3164 header
    // (timestamp + hostname), from it onwards is the CEF message.
    let (header, cef) = match after_pri.find("CEF:") {
        Some(idx) => (&after_pri[..idx], Some(&after_pri[idx..])),
        None => (after_pri, None),
    };

    let header_tokens: Vec<&str> = header.split_whitespace().collect();
    let timestamp = parse_timestamp(&header_tokens);
    let host_name = header_tokens.get(3).and_then(|h| clean_optional(h));

    let cef = cef.map(CefMessage::parse);
    let ext = cef.as_ref().map(|c| &c.extensions);

    let get = |key: &str| ext.and_then(|e| e.get(key)).map(String::as_str);

    let message = get("msg")
        .and_then(clean_optional)
        .or_else(|| cef.as_ref().and_then(|c| clean_optional(&c.name)))
        .unwrap_or_else(|| after_pri.trim().to_string());

    Ok(NormalizedEvent {
        timestamp,
        event_type: event_type(get("act"), &message),
        event_category: event_category(cef.as_ref(), &message),
        event_outcome: outcome(get("act"), get("outcome"), &message),
        source_ip: get("src").and_then(clean_optional),
        user_name: get("suser").and_then(clean_optional),
        host_name,
        log_level: Some(severity_name(pri & 7).to_string()),
        message,
    })
}

/// Splits a leading `<PRI>` off the line, returning the parsed priority (default
/// 13 — user.notice) and the remainder.
fn strip_pri(raw: &str) -> (u8, &str) {
    if let Some(rest) = raw.strip_prefix('<') {
        if let Some(end) = rest.find('>') {
            if let Ok(pri) = rest[..end].parse::<u16>() {
                return ((pri & 0xff) as u8, &rest[end + 1..]);
            }
        }
    }
    (13, raw)
}

/// Builds an ISO 8601 UTC timestamp from the RFC 3164 `Mmm dd HH:MM:SS` header
/// tokens. RFC 3164 omits the year and timezone, so we assume the current year
/// and UTC; any parse failure falls back to the current time.
fn parse_timestamp(header_tokens: &[&str]) -> String {
    let parsed = match header_tokens {
        [month, day, time, ..] => {
            let stamp = format!("{} {month} {day} {time}", Utc::now().year());
            NaiveDateTime::parse_from_str(&stamp, "%Y %b %d %H:%M:%S")
                .ok()
                .map(|naive| Utc.from_utc_datetime(&naive))
        }
        _ => None,
    };

    parsed
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

fn event_type(act: Option<&str>, message: &str) -> EventType {
    if let Some(act) = act {
        let act = act.to_lowercase();
        if act.contains("allow") {
            return EventType::Allowed;
        }
        if act.contains("deny") || act.contains("denied") || act.contains("block") {
            return EventType::Denied;
        }
    }

    let msg = message.to_lowercase();
    if is_auth(&msg) {
        if msg.contains("logoff") || msg.contains("logout") || msg.contains("log off") {
            EventType::End
        } else {
            EventType::Start
        }
    } else {
        EventType::Info
    }
}

fn event_category(cef: Option<&CefMessage>, message: &str) -> EventCategory {
    let mut haystack = message.to_lowercase();
    if let Some(cef) = cef {
        haystack.push(' ');
        haystack.push_str(&cef.signature_id.to_lowercase());
        haystack.push(' ');
        haystack.push_str(&cef.name.to_lowercase());
    }

    if is_auth(&haystack) {
        EventCategory::Authentication
    } else if haystack.contains("traffic") || haystack.contains("connection") {
        EventCategory::Network
    } else {
        EventCategory::Host
    }
}

fn outcome(act: Option<&str>, outcome: Option<&str>, message: &str) -> EventOutcome {
    let haystack = format!(
        "{} {} {}",
        act.unwrap_or(""),
        outcome.unwrap_or(""),
        message
    )
    .to_lowercase();

    if haystack.contains("success") || haystack.contains("allow") {
        EventOutcome::Success
    } else if haystack.contains("failure")
        || haystack.contains("fail")
        || haystack.contains("deny")
        || haystack.contains("denied")
        || haystack.contains("block")
    {
        EventOutcome::Failure
    } else {
        EventOutcome::Unknown
    }
}

/// True when the text carries authentication-related wording (tolerant of the
/// "logon" / "log on" / "logged on" spelling variants seen in CEF names).
fn is_auth(haystack: &str) -> bool {
    ["logon", "login", "log on", "logged on", "auth"]
        .iter()
        .any(|kw| haystack.contains(kw))
}

/// Maps a syslog severity (PRI & 7) to its standard name.
fn severity_name(severity: u8) -> &'static str {
    match severity {
        0 => "emergency",
        1 => "alert",
        2 => "critical",
        3 => "error",
        4 => "warning",
        5 => "notice",
        6 => "info",
        _ => "debug",
    }
}

/// A parsed CEF message: the header's signature id and name, plus the extension
/// key/value map. Other header fields aren't needed for the current mapping.
struct CefMessage {
    signature_id: String,
    name: String,
    extensions: HashMap<String, String>,
}

impl CefMessage {
    /// Parses `CEF:0|vendor|product|ver|sigid|name|sev|ext...`.
    fn parse(cef: &str) -> Self {
        let fields: Vec<&str> = cef.splitn(8, '|').collect();
        CefMessage {
            signature_id: fields.get(4).unwrap_or(&"").to_string(),
            name: fields.get(5).unwrap_or(&"").to_string(),
            extensions: parse_extensions(fields.get(7).unwrap_or(&"")),
        }
    }
}

/// Parses CEF extensions, which are space-separated `key=value` pairs whose
/// values may themselves contain spaces. A token of the form `key=...` (with an
/// alphanumeric key) starts a new field; any following token without such a
/// prefix is appended to the current value.
fn parse_extensions(ext: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut current: Option<String> = None;

    for token in ext.split_whitespace() {
        match token.find('=').map(|eq| (&token[..eq], &token[eq + 1..])) {
            Some((key, value)) if is_key(key) => {
                map.insert(key.to_string(), value.to_string());
                current = Some(key.to_string());
            }
            _ => {
                if let Some(value) = current.as_ref().and_then(|k| map.get_mut(k)) {
                    value.push(' ');
                    value.push_str(token);
                }
            }
        }
    }

    map
}

fn is_key(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_first_line(contents: &str) -> NormalizedEvent {
        let line = contents.lines().next().expect("a line");
        parse(line).expect("sample should parse")
    }

    #[test]
    fn sample_1_successful_logon_allowed() {
        let event = parse_first_line(include_str!("../../test/data/syslog/sample-1.log"));
        assert_eq!(event.event_type, EventType::Allowed); // act=allow takes precedence
        assert_eq!(event.event_category, EventCategory::Authentication);
        assert_eq!(event.event_outcome, EventOutcome::Success);
        assert_eq!(event.source_ip.as_deref(), Some("10.0.50.42"));
        assert_eq!(event.user_name.as_deref(), Some("jsmith"));
        assert_eq!(event.host_name.as_deref(), Some("192.168.1.1"));
        assert_eq!(event.log_level.as_deref(), Some("info")); // pri 134 & 7 = 6
        assert_eq!(event.message, "An account was successfully logged on");
    }

    #[test]
    fn sample_2_failed_logon_denied() {
        let event = parse_first_line(include_str!("../../test/data/syslog/sample-2.log"));
        assert_eq!(event.event_type, EventType::Denied); // act=denied
        assert_eq!(event.event_category, EventCategory::Authentication);
        assert_eq!(event.event_outcome, EventOutcome::Failure);
        assert_eq!(event.source_ip.as_deref(), Some("10.99.0.55"));
        assert_eq!(event.user_name.as_deref(), Some("admin"));
        assert_eq!(event.log_level.as_deref(), Some("notice")); // pri 133 & 7 = 5
        assert_eq!(event.message, "An account failed to log on");
    }

    #[test]
    fn sample_3_traffic_is_network() {
        let event = parse_first_line(include_str!("../../test/data/syslog/sample-3.log"));
        assert_eq!(event.event_type, EventType::Allowed);
        assert_eq!(event.event_category, EventCategory::Network); // sigid TRAFFIC
        assert_eq!(event.event_outcome, EventOutcome::Success);
        assert_eq!(event.source_ip.as_deref(), Some("192.168.1.100"));
        assert_eq!(event.message, "Connection allowed");
    }

    #[test]
    fn timestamp_is_iso8601_utc_with_current_year() {
        let event = parse_first_line(include_str!("../../test/data/syslog/sample-1.log"));
        let year = Utc::now().year();
        assert_eq!(event.timestamp, format!("{year}-12-05T10:30:45.000Z"));
    }

    #[test]
    fn extensions_preserve_spaces_in_values() {
        let ext = parse_extensions("src=1.2.3.4 msg=An account failed to log on act=denied");
        assert_eq!(ext.get("src").map(String::as_str), Some("1.2.3.4"));
        assert_eq!(
            ext.get("msg").map(String::as_str),
            Some("An account failed to log on")
        );
        assert_eq!(ext.get("act").map(String::as_str), Some("denied"));
    }

    #[test]
    fn pri_defaults_when_malformed() {
        let (pri, rest) = strip_pri("no pri here");
        assert_eq!(pri, 13);
        assert_eq!(rest, "no pri here");
    }
}
