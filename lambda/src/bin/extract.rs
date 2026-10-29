//! Local harness: `extract file.pdf [out_dir]` writes <name>.json / <name>.md and
//! prints wall time and peak RSS, to size the Lambda's memory against real documents.
use pdf_extract_lambda::{Extraction, Options, extract, peak_rss_mb};
use std::{
    env,
    error::Error,
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const USAGE: &str = "usage: extract <file.pdf> [out_dir]";

fn main() -> Result<(), Box<dyn Error>> {
    let (path, out_dir) = parse_args(env::args_os().skip(1))?;
    require_pdf(&path)?;
    fs::create_dir_all(&out_dir).map_err(at(&out_dir))?;

    let started = Instant::now();
    let result = extract(fs::read(&path).map_err(at(&path))?, &Options::default())?;

    write_outputs(
        &result,
        &out_dir.join(path.file_name().ok_or("path has no file name")?),
    )?;
    eprintln!("{}", summary(&result, started.elapsed()));
    Ok(())
}

fn parse_args(args: impl Iterator<Item = OsString>) -> Result<(PathBuf, PathBuf), &'static str> {
    let mut paths = args.map(PathBuf::from);
    let input = paths.next().ok_or(USAGE)?;
    Ok((input, paths.next().unwrap_or_else(|| ".".into())))
}

/// Writes `<out>.json` and `<out>.md`, where `out` is the output path minus its extension.
fn write_outputs(result: &Extraction, out: &Path) -> Result<(), Box<dyn Error>> {
    [
        ("json", serde_json::to_vec_pretty(result)?),
        ("md", result.markdown().into_bytes()),
    ]
    .into_iter()
    .try_for_each(|(ext, body)| fs::write(out.with_extension(ext), body))
    .map_err(Into::into)
}

fn summary(result: &Extraction, elapsed: Duration) -> String {
    format!(
        "{} pages, {} failed, {} likely scanned, {elapsed:?}, peak RSS {}",
        result.page_count(),
        result.pages_failed(),
        result.pages_scanned(),
        peak_rss_mb().map_or("n/a".into(), |mb| format!("{mb} MiB")),
    )
}

/// Prefixes an I/O error with the path it concerns, for `map_err`.
fn at(path: &Path) -> impl Fn(io::Error) -> String + '_ {
    move |e| format!("{}: {e}", path.display())
}

/// Guard: the output reuses the input's name with a new extension, so a non-PDF input could be overwritten.
fn require_pdf(path: &Path) -> Result<(), String> {
    path.extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("pdf"))
        .then_some(())
        .ok_or_else(|| format!("{}: not a .pdf file", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{parse_args, require_pdf, summary};
    use pdf_extract_lambda::{Options, extract};
    use std::{ffi::OsString, path::Path, time::Duration};

    fn args(a: &[&str]) -> impl Iterator<Item = OsString> {
        a.iter().map(OsString::from).collect::<Vec<_>>().into_iter()
    }

    #[test]
    fn out_dir_defaults_to_the_current_directory() {
        let (input, out) = parse_args(args(&["a.pdf"])).unwrap();
        assert_eq!(
            (input.as_path(), out.as_path()),
            (Path::new("a.pdf"), Path::new("."))
        );
        let (_, out) = parse_args(args(&["a.pdf", "dir"])).unwrap();
        assert_eq!(out, Path::new("dir"));
        assert!(parse_args(args(&[])).is_err());
    }

    #[test]
    fn only_pdfs_are_accepted() {
        assert!(require_pdf(Path::new("a.PDF")).is_ok());
        assert!(require_pdf(Path::new("a.md")).is_err());
        assert!(require_pdf(Path::new("pdf")).is_err());
    }

    #[test]
    fn summary_reports_the_counts() {
        let fixture = include_bytes!("../../tests/fixtures/1.pdf").to_vec();
        let result = extract(fixture, &Options::default()).unwrap();
        let line = summary(&result, Duration::from_secs(1));
        assert!(
            line.starts_with("7 pages, 0 failed, 0 likely scanned, 1s"),
            "{line}"
        );
    }
}
