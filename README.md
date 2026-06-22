# rust-log-normalizer

A TCP service that normalizes RFC 3164 syslog (wrapping CEF) and NDJSON (Windows Event Log)
event streams into a common, flat JSON schema.

Each connection is read line-by-line, its format is detected from the first usable line and
locked for the life of the connection, every line is transformed into a normalized record, and
records are written as NDJSON (one JSON object per line) to the configured sink.

## Build & test

```sh
cargo build --release
cargo test
```

## Run

```sh
# Listen on the default port (5044), write normalized records to stdout
cargo run --release

# Listen on a custom port and append records to a file
cargo run --release -- --port 6000 --output /var/log/normalized.ndjson
```

Send events with any TCP client (NDJSON must be one compact object per line):

```sh
printf '<134>Dec 05 10:30:45 host CEF:0|Vendor|Product|1.0|4624|logon|6|src=10.0.0.1 act=allow\n' | nc localhost 5044
jq -c . event.json | nc localhost 5044
```

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--bind` | `0.0.0.0` | Address to bind the listener to. |
| `-p`, `--port` | `5044` | TCP port to listen on. |
| `-o`, `--output` | `-` | Output destination: `-` for stdout, or a file path (append, created if missing). |
| `--max-connections` | `1024` | Max connections handled concurrently; at the limit new connections wait in the OS backlog. |
| `--max-line-bytes` | `1048576` | Max bytes for a single line; a longer line closes the connection. |
| `--queue-capacity` | `256` | Capacity (in batches) of the queue between connections and the sink. |

Logs are written to **stderr** (so stdout stays a clean NDJSON stream) and controlled with
`RUST_LOG`, e.g. `RUST_LOG=debug` to see per-connection events. Default level is `info`.

## Output schema

Records use flat, dotted top-level keys. Required fields are always present; optional fields are
omitted when unavailable.

| Field | Required | Notes |
| --- | --- | --- |
| `@timestamp` | yes | ISO 8601 UTC, millisecond precision. |
| `event.type` | yes | `start` \| `end` \| `info` \| `denied` \| `allowed`. |
| `event.category` | yes | `authentication` \| `network` \| `process` \| `host`. |
| `event.outcome` | yes | `success` \| `failure` \| `unknown`. |
| `source.ip` | no | Source IP when available. |
| `user.name` | no | Account name when available. |
| `host.name` | no | Host/computer name. |
| `log.level` | no | Severity string. |
| `message` | yes | Original or normalized message text. |

Example:

```json
{"@timestamp":"2026-02-14T15:45:33.221Z","event.type":"start","event.category":"authentication","event.outcome":"failure","source.ip":"10.99.0.55","user.name":"admin","host.name":"dc01.contoso.local","log.level":"info","message":"An account failed to log on."}
```

## Behavior & guarantees

- **Format detection** is per connection: the first non-empty line's leading byte (`<` → syslog,
  `{` → NDJSON) locks the format; subsequent lines are routed to that transform.
- **Backpressure is lossless.** Connections feed a single sink-writer task over a bounded channel;
  when the queue is full producers await, which stops reading the socket and propagates
  backpressure to the TCP sender.
- **Batching.** Connections batch lines (bounded by count, bytes, and a short timeout); the sink
  coalesces ready batches into one buffered write + flush.
- **Limits.** Concurrency is capped with a semaphore; a single over-limit line closes its
  connection (a well-behaved client reconnects).
- **Resilience.** Undetectable or malformed lines are logged and dropped without dropping the
  connection. Transient `accept()` errors are logged and retried. A sink write failure stops the
  sink loudly rather than silently discarding records.
- **Graceful shutdown.** On Ctrl-C / SIGTERM the listener stops accepting, in-flight batches are
  flushed, and the sink is drained before exit.

## Notes on mapping

- **Syslog timestamps** (RFC 3164) carry no year or timezone; the current year and UTC are
  assumed, and parsing failures fall back to the current time (per spec).
- For CEF, `event.type` follows `act=` (`allow`→`allowed`, `deny`/`block`→`denied`) before
  message-based auth classification, per the mapping spec.
- `S-1-0-0` SIDs, `-`, and empty strings are treated as "not applicable" and omitted from optional
  fields.
