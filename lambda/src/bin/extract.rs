//! Local harness: `extract file.pdf [out_dir]` writes <name>.json / <name>.md and
//! prints wall time and peak RSS, to size the Lambda's memory against real documents.
use pdf_extract_lambda::{Options, extract};
use std::{env, error::Error, fs, path::Path, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let path = args.next().ok_or("usage: extract <file.pdf> [out_dir]")?;
    let out_dir = args.next().unwrap_or_else(|| ".".into());

    let started = Instant::now();
    let result = extract(fs::read(&path)?, &Options::default())?;

    let name = Path::new(&path)
        .file_name()
        .ok_or("path has no file name")?;
    let out = Path::new(&out_dir).join(name);
    fs::write(out.with_extension("json"), serde_json::to_vec(&result)?)?;
    fs::write(out.with_extension("md"), result.markdown())?;

    eprintln!(
        "{} pages, {} failed, {} likely scanned, {:?}, peak RSS {}",
        result.page_count,
        result.pages_failed(),
        result.pages_scanned(),
        started.elapsed(),
        peak_rss_mb().map_or("n/a".into(), |mb| format!("{mb} MiB")),
    );
    Ok(())
}

/// High-water resident set size from `/proc/self/status` (Linux only).
fn peak_rss_mb() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find_map(|l| l.strip_prefix("VmHWM:"))?;
    Some(line.split_whitespace().next()?.parse::<u64>().ok()? / 1024)
}
