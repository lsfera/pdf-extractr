//! SQS (carrying S3 ObjectCreated notifications) -> extract -> write `<OUTPUT_PREFIX><key>.json` and `.md` to OUTPUT_BUCKET.
//! Env: OUTPUT_BUCKET (required), OUTPUT_PREFIX (default "extracted/"), MAX_PDF_BYTES (default 100 MiB),
//!      PDF_PASSWORD, TABLES_JSON=0 to skip the table grids, S3_FORCE_PATH_STYLE=1 for LocalStack/MinIO.
//! Use a different bucket (or a prefix excluded from the trigger) for output to avoid loops.
//!
//! Observability: JSON `tracing` logs (set the function's log format to JSON, level via RUST_LOG) and
//! CloudWatch Embedded Metric Format lines on stdout (namespace `PdfExtract`, dimension `Function`).
//! `PeakRssMb` is the process's high-water RSS (`VmHWM`) after each document. A warm container keeps its
//! earlier peaks, so read it as "at least this much"; Lambda's REPORT line ("Max Memory Used") is the same figure.
//!
//! Local debugging: with LOCAL_S3_BUCKET and LOCAL_S3_KEY set, the binary processes that one object and exits,
//! without the Lambda runtime API (see .vscode/launch.json).
use aws_lambda_events::event::{s3::S3Event, sqs::SqsEvent};
use aws_sdk_s3::{Client, primitives::ByteStream};
use lambda_runtime::tracing::{self, Instrument, error, info, info_span, warn};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use pdf_extract_lambda::{Extraction, Options, extract};
use serde_json::json;
use std::env;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Settings read from the environment once, at cold start.
struct Config {
    out_bucket: String,
    prefix: String,
    max_pdf_bytes: i64,
    opts: Options,
}

impl Config {
    fn from_env() -> Result<Self, Error> {
        Ok(Self {
            out_bucket: env::var("OUTPUT_BUCKET").map_err(|_| "OUTPUT_BUCKET is not set")?,
            prefix: env::var("OUTPUT_PREFIX").unwrap_or_else(|_| "extracted/".into()),
            max_pdf_bytes: env::var("MAX_PDF_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100 << 20),
            opts: Options {
                tables_json: env::var("TABLES_JSON").map_or(true, |v| v != "0"),
                password: env::var("PDF_PASSWORD").ok(),
            },
        })
    }
}

struct Stats {
    pdf_bytes: usize,
    pages: usize,
    pages_failed: usize,
    pages_scanned: usize,
    tables: usize,
    extract_ms: u64,
    peak_rss_mb: Option<u64>,
}

impl Stats {
    fn new(pdf_bytes: usize, e: &Extraction, extract_ms: u64) -> Self {
        Self {
            pdf_bytes,
            pages: e.page_count,
            pages_failed: e.pages_failed(),
            pages_scanned: e.pages_scanned(),
            tables: e.table_count(),
            extract_ms,
            peak_rss_mb: peak_rss_mb(),
        }
    }
}

/// High-water resident set size in MiB, from `/proc/self/status` (Linux only).
fn peak_rss_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse()
        .ok()?;
    Some(kb / 1024)
}

/// How SQS should treat a failed object.
enum Failure {
    /// Retrying cannot help (corrupt or oversize PDF, wrong password): log, count and acknowledge.
    Permanent(Error),
    /// S3 hiccup or similar: fail the invocation so SQS redelivers, and eventually dead-letters, the message.
    Transient(Error),
}

fn transient(e: impl Into<Error>) -> Failure {
    Failure::Transient(e.into())
}

fn permanent(e: impl Into<Error>) -> Failure {
    Failure::Permanent(e.into())
}

/// Each SQS message body is an S3 event notification. Malformed messages and unprocessable PDFs are
/// acknowledged after logging; only transient errors fail the invocation. Every record is attempted
/// before the first transient error is returned.
async fn handler(client: &Client, cfg: &Config, ev: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    let mut retry = None;
    for msg in ev.payload.records {
        let Some(body) = msg.body else { continue };
        let event = match parse_notification(&body) {
            Ok(Some(event)) => event,
            Ok(None) => continue,
            Err(e) => {
                error!(error = %e, "unparseable message body; dropping it");
                emit_metrics("<unparseable message>", None);
                continue;
            }
        };
        if let Err(e) = handle_s3(client, cfg, event).await {
            retry.get_or_insert(e);
        }
    }
    retry.map_or(Ok(()), Err)
}

/// `Ok(None)` for the `s3:TestEvent` S3 sends once, when the notification is first configured.
fn parse_notification(body: &str) -> Result<Option<S3Event>, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_str(body)?;
    if value.get("Event").and_then(|e| e.as_str()) == Some("s3:TestEvent") {
        warn!("skipping s3:TestEvent");
        return Ok(None);
    }
    serde_json::from_value(value).map(Some)
}

async fn handle_s3(client: &Client, cfg: &Config, event: S3Event) -> Result<(), Error> {
    let mut retry = None;
    for rec in event.records {
        let (Some(bucket), Some(raw_key)) = (rec.s3.bucket.name, rec.s3.object.key) else {
            continue;
        };
        // S3 event keys are URL-encoded ('+' for space, %XX).
        let key = decode_key(&raw_key);

        let started = Instant::now();
        let span = info_span!("pdf", %bucket, %key);
        match process(client, cfg, &bucket, &key).instrument(span).await {
            Ok(stats) => {
                info!(
                    %bucket,
                    %key,
                    pages = stats.pages,
                    failed = stats.pages_failed,
                    scanned = stats.pages_scanned,
                    tables = stats.tables,
                    extract_ms = stats.extract_ms,
                    total_ms = elapsed_ms(started),
                    "pdf extracted"
                );
                emit_metrics(&key, Some(&stats));
            }
            Err(Failure::Permanent(e)) => {
                error!(%bucket, %key, error = %e, "pdf cannot be processed; not retrying");
                emit_metrics(&key, None);
            }
            Err(Failure::Transient(e)) => {
                error!(%bucket, %key, error = %e, "pdf extraction failed; will retry");
                emit_metrics(&key, None);
                retry.get_or_insert(e);
            }
        }
    }
    retry.map_or(Ok(()), Err)
}

async fn process(client: &Client, cfg: &Config, bucket: &str, key: &str) -> Result<Stats, Failure> {
    let obj = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(transient)?;
    let content_length = match obj.content_length() {
        Some(len) if len <= cfg.max_pdf_bytes => len,
        Some(_) => return Err(permanent(format!("s3://{bucket}/{key} exceeds MAX_PDF_BYTES"))),
        None => return Err(permanent(format!("s3://{bucket}/{key} has no content length"))),
    };
    // Read straight into a right-sized Vec: `collect()` would hold every chunk and then copy them into one buffer.
    let mut bytes = Vec::with_capacity(usize::try_from(cfg.max_pdf_bytes.min(content_length)).unwrap_or(0));
    let mut body = obj.body;
    while let Some(chunk) = body.try_next().await.map_err(transient)? {
        bytes.extend_from_slice(&chunk);
    }
    let pdf_bytes = bytes.len();

    let started = Instant::now();
    let result = extract(bytes, &cfg.opts).map_err(permanent)?;
    let stats = Stats::new(pdf_bytes, &result, elapsed_ms(started));
    let json = serde_json::to_vec(&result).map_err(permanent)?;
    let md = result.markdown();
    drop(result);

    for (ext, content_type, body) in [
        ("json", "application/json", json),
        ("md", "text/markdown", md.into_bytes()),
    ] {
        client
            .put_object()
            .bucket(&cfg.out_bucket)
            .key(format!("{}{key}.{ext}", cfg.prefix))
            .content_type(content_type)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(transient)?;
    }
    Ok(stats)
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// CloudWatch Embedded Metric Format: one JSON line on stdout becomes metrics, with no API calls.
/// The S3 key is a plain property (searchable in Logs Insights), not a dimension, to keep cardinality low.
/// `stats` is `None` when the PDF failed.
fn emit_metrics(key: &str, stats: Option<&Stats>) {
    let mut values = vec![("Failures", "Count", u64::from(stats.is_none()))];
    if let Some(s) = stats {
        values.extend([
            ("Pages", "Count", s.pages as u64),
            ("PagesFailed", "Count", s.pages_failed as u64),
            ("PagesScanned", "Count", s.pages_scanned as u64),
            ("Tables", "Count", s.tables as u64),
            ("PdfBytes", "Bytes", s.pdf_bytes as u64),
            ("ExtractMs", "Milliseconds", s.extract_ms),
        ]);
        if let Some(mb) = s.peak_rss_mb {
            values.push(("PeakRssMb", "Megabytes", mb));
        }
    }
    let definitions: Vec<_> = values
        .iter()
        .map(|&(name, unit, _)| json!({ "Name": name, "Unit": unit }))
        .collect();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64);

    let mut line = json!({
        "_aws": {
            "Timestamp": timestamp,
            "CloudWatchMetrics": [{
                "Namespace": "PdfExtract",
                "Dimensions": [["Function"]],
                "Metrics": definitions,
            }],
        },
        "Function": env::var("AWS_LAMBDA_FUNCTION_NAME").unwrap_or_default(),
        "s3_key": key,
    });
    for (name, _, value) in values {
        line[name] = value.into();
    }
    println!("{line}");
}

fn decode_key(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let hex = b
            .get(i + 1..i + 3)
            .and_then(|h| std::str::from_utf8(h).ok())
            .and_then(|h| u8::from_str_radix(h, 16).ok());
        match (b[i], hex) {
            (b'+', _) => out.push(b' '),
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 2;
            }
            (c, _) => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();
    let cfg = Config::from_env()?;
    let aws = aws_config::load_from_env().await;
    let s3_config = aws_sdk_s3::config::Builder::from(&aws)
        .force_path_style(env::var("S3_FORCE_PATH_STYLE").is_ok_and(|v| v == "1"))
        .build();
    let client = Client::from_conf(s3_config);

    if let (Ok(bucket), Ok(key)) = (env::var("LOCAL_S3_BUCKET"), env::var("LOCAL_S3_KEY")) {
        let stats = process(&client, &cfg, &bucket, &key).await;
        emit_metrics(&key, stats.as_ref().ok());
        return stats.map(drop).map_err(|(Failure::Permanent(e) | Failure::Transient(e))| e);
    }
    lambda_runtime::run(service_fn(|ev| handler(&client, &cfg, ev))).await
}

#[cfg(test)]
mod tests {
    use super::{decode_key, parse_notification};

    #[test]
    fn s3_test_event_is_skipped_and_garbage_is_rejected() {
        assert!(matches!(parse_notification(r#"{"Event":"s3:TestEvent"}"#), Ok(None)));
        assert!(parse_notification("not json").is_err());
        assert!(parse_notification(r#"{"unrelated":true}"#).is_err());
    }

    #[test]
    fn decodes_s3_event_keys() {
        assert_eq!(decode_key("a+b%2Fc%C3%A9.pdf"), "a b/cé.pdf");
        assert_eq!(decode_key("100%.pdf"), "100%.pdf");
        assert_eq!(decode_key("tail%2"), "tail%2");
    }
}
