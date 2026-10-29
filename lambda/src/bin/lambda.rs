//! SQS (carrying S3 `ObjectCreated` notifications) -> extract -> write `<OUTPUT_PREFIX><key>.json` and `.md` to `OUTPUT_BUCKET`.
//! Env: `OUTPUT_BUCKET` (required), `OUTPUT_PREFIX` (default "extracted/"), `MAX_PDF_BYTES` (default 100 MiB),
//!      `PDF_PASSWORD`, `TABLES_JSON=0` to skip the table grids, `S3_FORCE_PATH_STYLE=1` for Floci/MinIO.
//! Use a different bucket (or a prefix excluded from the trigger) for output to avoid loops.
//! The event source mapping must set `ReportBatchItemFailures`: only the messages that hit a transient
//! error are returned to the queue, so a retry never reprocesses PDFs that already succeeded.
//!
//! Observability: JSON `tracing` logs (set the function's log format to JSON, level via `RUST_LOG`) and
//! `CloudWatch` Embedded Metric Format lines on stdout (namespace `PdfExtract`, dimension `Function`).
//! `PeakRssMb` is the process's high-water RSS (`VmHWM`) after each document. A warm container keeps its
//! earlier peaks, so read it as "at least this much"; Lambda's REPORT line ("Max Memory Used") is the same figure.
//!
//! Local debugging: with `LOCAL_S3_BUCKET` and `LOCAL_S3_KEY` set, the binary processes that one object and exits,
//! without the Lambda runtime API (see .vscode/launch.json).
use aws_lambda_events::event::{
    s3::S3Event,
    sqs::{BatchItemFailure, SqsBatchResponse, SqsEvent},
};
use aws_sdk_s3::{
    Client, error::SdkError, operation::get_object::GetObjectError, primitives::ByteStream,
};
use lambda_runtime::tracing::{self, Instrument, error, info, info_span, warn};
use lambda_runtime::{Error, LambdaEvent, service_fn};
use pdf_extract_lambda::{ExtractError, Extraction, Options, extract, peak_rss_mb};
use serde_json::json;
use std::env;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Time kept back from the Lambda deadline for serialising and uploading the results.
const TIME_RESERVE: Duration = Duration::from_secs(15);

/// Invalid or missing environment settings, detected at cold start.
#[derive(Debug, thiserror::Error)]
enum ConfigError {
    #[error("OUTPUT_BUCKET is not set")]
    MissingOutputBucket,
    #[error("MAX_PDF_BYTES is not an integer: {0:?}")]
    InvalidMaxPdfBytes(String),
}

/// Why one S3 object could not be turned into output. [`ProcessError::is_transient`] tells SQS how to react.
#[derive(Debug, thiserror::Error)]
enum ProcessError {
    #[error(
        "s3://{bucket}/{key} is under the output prefix; refusing to re-process our own output"
    )]
    OutputLoop { bucket: String, key: String },
    #[error("s3://{bucket}/{key} is {len} bytes, over MAX_PDF_BYTES ({max})")]
    TooLarge {
        bucket: String,
        key: String,
        len: u64,
        max: u64,
    },
    #[error("s3://{bucket}/{key} has no content length")]
    MissingContentLength { bucket: String, key: String },
    #[error("s3://{bucket}/{key} cannot be read: {source}")]
    ObjectUnavailable {
        bucket: String,
        key: String,
        source: Error,
    },
    #[error("extraction task failed: {0}")]
    Worker(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Extract(#[from] ExtractError),
    #[error("cannot serialise the extraction: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("cannot download s3://{bucket}/{key}: {source}")]
    Download {
        bucket: String,
        key: String,
        source: Error,
    },
    #[error("cannot upload s3://{bucket}/{key}: {source}")]
    Upload {
        bucket: String,
        key: String,
        source: Error,
    },
}

impl ProcessError {
    fn download(bucket: &str, key: &str, source: impl Into<Error>) -> Self {
        Self::Download {
            bucket: bucket.into(),
            key: key.into(),
            source: source.into(),
        }
    }

    /// A 4xx from `GetObject` (deleted object, access denied) fails the same way on every retry;
    /// throttling, timeouts and 5xx do not.
    fn get_object(bucket: &str, key: &str, e: SdkError<GetObjectError>) -> Self {
        let permanent = e
            .raw_response()
            .map(|r| r.status().as_u16())
            .is_some_and(|status| (400..500).contains(&status) && status != 408 && status != 429);
        if permanent {
            Self::ObjectUnavailable {
                bucket: bucket.into(),
                key: key.into(),
                source: e.into(),
            }
        } else {
            Self::download(bucket, key, e)
        }
    }

    fn upload(bucket: &str, key: &str, source: impl Into<Error>) -> Self {
        Self::Upload {
            bucket: bucket.into(),
            key: key.into(),
            source: source.into(),
        }
    }

    /// S3 hiccups are worth a redelivery (and, eventually, the dead-letter queue). Everything else
    /// (corrupt or oversize PDF, wrong password, our own output) fails the same way every time:
    /// log, count and acknowledge.
    fn is_transient(&self) -> bool {
        matches!(self, Self::Download { .. } | Self::Upload { .. })
    }
}

/// Settings read from the environment once, at cold start.
struct Config {
    out_bucket: String,
    prefix: String,
    max_pdf_bytes: u64,
    opts: Options,
}

impl Config {
    fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    /// `from_env` with the environment abstracted away, so tests need not mutate the process environment.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        Ok(Self {
            out_bucket: get("OUTPUT_BUCKET").ok_or(ConfigError::MissingOutputBucket)?,
            prefix: normalize_prefix(get("OUTPUT_PREFIX").unwrap_or_else(|| "extracted/".into())),
            max_pdf_bytes: match get("MAX_PDF_BYTES") {
                Some(v) => v.parse().map_err(|_| ConfigError::InvalidMaxPdfBytes(v))?,
                None => 100 << 20,
            },
            opts: Options {
                tables_json: get("TABLES_JSON").is_none_or(|v| v != "0"),
                password: get("PDF_PASSWORD"),
                deadline: None,
            },
        })
    }
}

/// Ensures a non-empty prefix ends in `/`, so `out` + `a.pdf` cannot become `outa.pdf`.
fn normalize_prefix(mut prefix: String) -> String {
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    prefix
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
            pages: e.page_count(),
            pages_failed: e.pages_failed(),
            pages_scanned: e.pages_scanned(),
            tables: e.table_count(),
            extract_ms,
            peak_rss_mb: peak_rss_mb(),
        }
    }
}

/// Each SQS message body is an S3 event notification. Malformed messages and unprocessable PDFs are
/// acknowledged after logging; a message that hit a transient error is reported in
/// `batchItemFailures` so that SQS redelivers, and eventually dead-letters, only that message.
async fn handler(
    client: &Client,
    cfg: &Config,
    ev: LambdaEvent<SqsEvent>,
) -> Result<SqsBatchResponse, Error> {
    let mut response = SqsBatchResponse::default();
    let deadline = extraction_deadline(ev.context.deadline());
    for msg in ev.payload.records {
        if Box::pin(handle_message(client, cfg, msg.body.as_deref(), deadline))
            .await
            .is_err()
        {
            if let Some(id) = msg.message_id {
                response.batch_item_failures.push(batch_item_failure(id));
            } else {
                error!("failed message has no id, so it cannot be retried selectively");
            }
        }
    }
    Ok(response)
}

/// When to stop starting new pages: the invocation deadline minus [`TIME_RESERVE`].
fn extraction_deadline(invocation_deadline: SystemTime) -> Option<Instant> {
    let remaining = invocation_deadline.duration_since(SystemTime::now()).ok()?;
    Instant::now().checked_add(remaining.saturating_sub(TIME_RESERVE))
}

// `BatchItemFailure` is `#[non_exhaustive]`, so it cannot be built with a struct literal.
#[allow(clippy::field_reassign_with_default)]
fn batch_item_failure(id: String) -> BatchItemFailure {
    let mut failure = BatchItemFailure::default();
    failure.item_identifier = id;
    failure
}

/// `Err` only for a transient failure, the one case worth redelivering the message for.
async fn handle_message(
    client: &Client,
    cfg: &Config,
    body: Option<&str>,
    deadline: Option<Instant>,
) -> Result<(), ProcessError> {
    let Some(body) = body else {
        error!("message has no body; dropping it");
        emit_metrics("<message without body>", None);
        return Ok(());
    };
    match parse_notification(body) {
        Ok(Some(event)) => Box::pin(handle_s3(client, cfg, event, deadline)).await,
        Ok(None) => Ok(()),
        Err(e) => {
            error!(error = %e, "unparseable message body; dropping it");
            emit_metrics("<unparseable message>", None);
            Ok(())
        }
    }
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

async fn handle_s3(
    client: &Client,
    cfg: &Config,
    event: S3Event,
    deadline: Option<Instant>,
) -> Result<(), ProcessError> {
    let mut retry = None;
    for rec in event.records {
        let (Some(bucket), Some(raw_key)) = (rec.s3.bucket.name, rec.s3.object.key) else {
            error!("S3 record has no bucket or key; dropping it");
            emit_metrics("<record without bucket or key>", None);
            continue;
        };
        // S3 event keys are URL-encoded ('+' for space, %XX).
        let key = decode_key(&raw_key);

        let started = Instant::now();
        let span = info_span!("pdf", %bucket, %key);
        match Box::pin(process(client, cfg, &bucket, &key, deadline).instrument(span)).await {
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
            Err(e) if e.is_transient() => {
                error!(%bucket, %key, error = %e, "pdf extraction failed; will retry");
                emit_metrics(&key, None);
                retry.get_or_insert(e);
            }
            Err(e) => {
                error!(%bucket, %key, error = %e, "pdf cannot be processed; not retrying");
                emit_metrics(&key, None);
            }
        }
    }
    retry.map_or(Ok(()), Err)
}

async fn process(
    client: &Client,
    cfg: &Config,
    bucket: &str,
    key: &str,
    deadline: Option<Instant>,
) -> Result<Stats, ProcessError> {
    if bucket == cfg.out_bucket && key.starts_with(&cfg.prefix) {
        return Err(ProcessError::OutputLoop {
            bucket: bucket.into(),
            key: key.into(),
        });
    }
    let bytes = fetch_pdf(client, cfg, bucket, key).await?;
    let pdf_bytes = bytes.len();

    // Extraction is CPU-bound: keep it off the async executor.
    let opts = Options {
        deadline,
        ..cfg.opts.clone()
    };
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || extract(bytes, &opts)).await??;
    let stats = Stats::new(pdf_bytes, &result, elapsed_ms(started));
    let json = serde_json::to_vec(&result)?;
    let md = result.markdown();
    drop(result);

    Box::pin(upload_outputs(client, cfg, key, json, md.into_bytes())).await?;
    Ok(stats)
}

/// Downloads the object, enforcing `MAX_PDF_BYTES` against both the declared and the actual size.
async fn fetch_pdf(
    client: &Client,
    cfg: &Config,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>, ProcessError> {
    let obj = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| ProcessError::get_object(bucket, key, e))?;
    let too_large = |len| ProcessError::TooLarge {
        bucket: bucket.into(),
        key: key.into(),
        len,
        max: cfg.max_pdf_bytes,
    };
    let Some(content_length) = obj.content_length().and_then(|len| u64::try_from(len).ok()) else {
        return Err(ProcessError::MissingContentLength {
            bucket: bucket.into(),
            key: key.into(),
        });
    };
    if content_length > cfg.max_pdf_bytes {
        return Err(too_large(content_length));
    }
    // Read straight into a right-sized Vec: `collect()` would hold every chunk and then copy them into one buffer.
    let mut bytes = Vec::with_capacity(usize::try_from(content_length).unwrap_or(0));
    let mut body = obj.body;
    while let Some(chunk) = body
        .try_next()
        .await
        .map_err(|e| ProcessError::download(bucket, key, e))?
    {
        // The declared length is not trusted: stop as soon as the stream outgrows the cap.
        let len = to_u64(bytes.len() + chunk.len());
        if len > cfg.max_pdf_bytes {
            return Err(too_large(len));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Both uploads are idempotent, so a retry after a partial failure rewrites the pair.
async fn upload_outputs(
    client: &Client,
    cfg: &Config,
    key: &str,
    json: Vec<u8>,
    md: Vec<u8>,
) -> Result<(), ProcessError> {
    let put = |ext: &str, content_type: &'static str, body: Vec<u8>| {
        put_object(
            client,
            &cfg.out_bucket,
            format!("{}{key}.{ext}", cfg.prefix),
            content_type,
            body,
        )
    };
    tokio::try_join!(
        put("json", "application/json", json),
        put("md", "text/markdown", md),
    )
    .map(drop)
}

async fn put_object(
    client: &Client,
    bucket: &str,
    key: String,
    content_type: &str,
    body: Vec<u8>,
) -> Result<(), ProcessError> {
    client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .content_type(content_type)
        .body(ByteStream::from(body))
        .send()
        .await
        .map(drop)
        .map_err(|e| ProcessError::upload(bucket, &key, e))
}

fn to_u64(n: usize) -> u64 {
    u64::try_from(n).unwrap_or(u64::MAX)
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `CloudWatch` Embedded Metric Format: one JSON line on stdout becomes metrics, with no API calls.
/// The S3 key is a plain property (searchable in Logs Insights), not a dimension, to keep cardinality low.
/// `stats` is `None` when the PDF failed.
fn emit_metrics(key: &str, stats: Option<&Stats>) {
    let mut values = vec![("Failures", "Count", u64::from(stats.is_none()))];
    if let Some(s) = stats {
        values.extend([
            ("Pages", "Count", to_u64(s.pages)),
            ("PagesFailed", "Count", to_u64(s.pages_failed)),
            ("PagesScanned", "Count", to_u64(s.pages_scanned)),
            ("Tables", "Count", to_u64(s.tables)),
            ("PdfBytes", "Bytes", to_u64(s.pdf_bytes)),
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
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

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
        let stats = Box::pin(process(&client, &cfg, &bucket, &key, None)).await;
        emit_metrics(&key, stats.as_ref().ok());
        return stats.map(drop).map_err(Into::into);
    }
    lambda_runtime::run(service_fn(|ev| handler(&client, &cfg, ev))).await
}

#[cfg(test)]
mod tests {
    use super::{
        Config, ConfigError, ProcessError, decode_key, normalize_prefix, parse_notification,
    };
    use pdf_extract_lambda::{Options, extract};

    #[test]
    fn prefix_gets_a_trailing_slash() {
        assert_eq!(normalize_prefix("out".into()), "out/");
        assert_eq!(normalize_prefix("out/".into()), "out/");
        assert_eq!(normalize_prefix(String::new()), "");
    }

    #[test]
    fn s3_test_event_is_skipped_and_garbage_is_rejected() {
        assert!(matches!(
            parse_notification(r#"{"Event":"s3:TestEvent"}"#),
            Ok(None)
        ));
        assert!(parse_notification("not json").is_err());
        assert!(parse_notification(r#"{"unrelated":true}"#).is_err());
    }

    #[test]
    fn decodes_s3_event_keys() {
        assert_eq!(decode_key("a+b%2Fc%C3%A9.pdf"), "a b/cé.pdf");
        assert_eq!(decode_key("100%.pdf"), "100%.pdf");
        assert_eq!(decode_key("tail%2"), "tail%2");
    }

    #[test]
    fn only_s3_failures_are_transient() {
        let s3 = || std::io::Error::other("connection reset");
        assert!(ProcessError::download("b", "k", s3()).is_transient());
        assert!(ProcessError::upload("b", "k", s3()).is_transient());

        let corrupt = extract(b"not a pdf".to_vec(), &Options::default()).unwrap_err();
        assert!(!ProcessError::from(corrupt).is_transient());
        let too_large = ProcessError::TooLarge {
            bucket: "b".into(),
            key: "k".into(),
            len: 2,
            max: 1,
        };
        assert!(!too_large.is_transient());
        assert_eq!(
            too_large.to_string(),
            "s3://b/k is 2 bytes, over MAX_PDF_BYTES (1)"
        );
    }

    fn config(vars: &'static [(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(|name| {
            vars.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_string())
        })
    }

    #[test]
    fn config_defaults() {
        let cfg = config(&[("OUTPUT_BUCKET", "out")]).unwrap();
        assert_eq!(cfg.out_bucket, "out");
        assert_eq!(cfg.prefix, "extracted/");
        assert_eq!(cfg.max_pdf_bytes, 100 << 20);
        assert!(cfg.opts.tables_json);
        assert!(cfg.opts.password.is_none());
    }

    #[test]
    fn config_overrides() {
        let cfg = config(&[
            ("OUTPUT_BUCKET", "out"),
            ("OUTPUT_PREFIX", "x"),
            ("MAX_PDF_BYTES", "10"),
            ("TABLES_JSON", "0"),
            ("PDF_PASSWORD", "pw"),
        ])
        .unwrap();
        assert_eq!(cfg.prefix, "x/");
        assert_eq!(cfg.max_pdf_bytes, 10);
        assert!(!cfg.opts.tables_json);
        assert_eq!(cfg.opts.password.as_deref(), Some("pw"));
    }

    #[test]
    fn config_rejects_bad_values() {
        assert!(matches!(config(&[]), Err(ConfigError::MissingOutputBucket)));
        for bad in ["-1", "100MB", ""] {
            let vars: &'static [(&str, &str)] = match bad {
                "-1" => &[("OUTPUT_BUCKET", "o"), ("MAX_PDF_BYTES", "-1")],
                "100MB" => &[("OUTPUT_BUCKET", "o"), ("MAX_PDF_BYTES", "100MB")],
                _ => &[("OUTPUT_BUCKET", "o"), ("MAX_PDF_BYTES", "")],
            };
            assert!(matches!(
                config(vars),
                Err(ConfigError::InvalidMaxPdfBytes(_))
            ));
        }
    }
}
