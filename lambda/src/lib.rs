//! PDF -> structured JSON + Markdown, tuned for Lambda: one page is extracted,
//! serialised into the output and dropped before the next, and a failure (error
//! or panic) on one page is recorded instead of failing the whole document.

use pdf_oxide::converters::ConversionOptions;
use pdf_oxide::document::PdfDocument;
use pdf_oxide::structure::table_extractor as oxide;
use serde::Serialize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

/// Why one page produced no text (or no tables). Serialised as its message.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PageError {
    #[error("{0}")]
    Failed(String),
    #[error("panic during extraction")]
    Panicked,
    #[error("skipped: the document's time budget ran out")]
    TimedOut,
}

impl Serialize for PageError {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

/// Document-level failures. Page-level failures are recorded in [`Page::error`] instead.
#[derive(Debug, thiserror::Error)]
pub enum ExtractError {
    #[error("cannot open PDF: {0}")]
    Open(#[source] pdf_oxide::error::Error),
    #[error("cannot decrypt PDF: {0}")]
    Decrypt(#[source] pdf_oxide::error::Error),
    #[error("cannot decrypt PDF: password rejected")]
    PasswordRejected,
    #[error("cannot read page count: {0}")]
    PageCount(#[source] pdf_oxide::error::Error),
}

#[derive(Debug, Serialize)]
pub struct Extraction {
    pub pdf_version: String,
    pub encrypted: bool,
    // Private so `page_count` cannot drift from `pages`; both are exposed through getters.
    page_count: usize,
    pages: Vec<Page>,
}

impl Extraction {
    #[must_use]
    pub fn page_count(&self) -> usize {
        self.page_count
    }

    #[must_use]
    pub fn pages(&self) -> &[Page] {
        &self.pages
    }

    #[must_use]
    pub fn pages_failed(&self) -> usize {
        self.pages
            .iter()
            .filter(|p| p.error.is_some() || p.tables_error.is_some())
            .count()
    }

    #[must_use]
    pub fn pages_scanned(&self) -> usize {
        self.pages.iter().filter(|p| p.likely_scanned).count()
    }

    #[must_use]
    pub fn table_count(&self) -> usize {
        self.pages.iter().map(|p| p.tables.len()).sum()
    }

    /// All pages' Markdown joined, for the `.md` sidecar.
    #[must_use]
    pub fn markdown(&self) -> String {
        self.pages
            .iter()
            .map(|p| p.markdown.as_str())
            .collect::<Vec<_>>()
            .join("\n\n---\n\n")
    }
}

#[derive(Debug, Serialize)]
pub struct Page {
    /// 1-based page number.
    pub page: usize,
    pub markdown: String,
    pub tables: Vec<Table>,
    /// No text layer found: probably a scan, needs OCR downstream.
    pub likely_scanned: bool,
    /// Extraction time for this page.
    pub ms: u64,
    /// The page's text could not be extracted; `markdown` is empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<PageError>,
    /// Table detection failed; `markdown` is still valid but `tables` is empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables_error: Option<PageError>,
}

#[derive(Debug, Serialize)]
pub struct Table {
    /// x, y, width, height
    pub bbox: Option<[f32; 4]>,
    pub has_header: bool,
    pub cols: usize,
    pub rows: Vec<Vec<Cell>>,
}

#[derive(Debug, Serialize)]
pub struct Cell {
    pub text: String,
    pub colspan: u32,
    pub rowspan: u32,
    pub header: bool,
}

impl From<oxide::Table> for Table {
    fn from(t: oxide::Table) -> Self {
        Self {
            bbox: t.bbox.map(|r| [r.x, r.y, r.width, r.height]),
            has_header: t.has_header,
            cols: t.col_count,
            rows: t
                .rows
                .into_iter()
                .map(|row| row.cells.into_iter().map(Cell::from).collect())
                .collect(),
        }
    }
}

impl From<oxide::TableCell> for Cell {
    fn from(c: oxide::TableCell) -> Self {
        Self {
            text: c.text,
            colspan: c.colspan,
            rowspan: c.rowspan,
            header: c.is_header,
        }
    }
}

#[derive(Clone)]
pub struct Options {
    /// Also emit tables as cell grids. Table detection then runs twice per page
    /// (once for the Markdown, once here); turn off if only Markdown is needed.
    pub tables_json: bool,
    /// Password for encrypted documents.
    pub password: Option<String>,
    /// Pages not started by this instant are recorded as [`PageError::TimedOut`], so a slow document
    /// returns what it has instead of being killed with nothing.
    pub deadline: Option<Instant>,
}

/// Hand-written so the password never reaches a log line.
impl std::fmt::Debug for Options {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Options")
            .field("tables_json", &self.tables_json)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            tables_json: true,
            password: None,
            deadline: None,
        }
    }
}

/// Extracts every page of `bytes`.
///
/// # Errors
///
/// Fails if the PDF cannot be opened or decrypted; a bad page does not fail the document.
pub fn extract(bytes: Vec<u8>, opts: &Options) -> Result<Extraction, ExtractError> {
    let doc = PdfDocument::from_bytes(bytes).map_err(ExtractError::Open)?;
    let encrypted = doc.is_encrypted();
    if encrypted {
        // An empty user password is common for "protected" statements.
        let password = opts.password.as_deref().unwrap_or_default();
        if !doc
            .authenticate(password.as_bytes())
            .map_err(ExtractError::Decrypt)?
        {
            return Err(ExtractError::PasswordRejected);
        }
    }
    let page_count = doc.page_count().map_err(ExtractError::PageCount)?;
    let (major, minor) = doc.version();
    let md_opts = ConversionOptions::default();

    let pages = (0..page_count)
        .map(|idx| {
            if opts.deadline.is_some_and(|d| Instant::now() >= d) {
                skipped_page(idx)
            } else {
                process_page(&doc, idx, &md_opts, opts)
            }
        })
        .collect();

    Ok(Extraction {
        pdf_version: format!("{major}.{minor}"),
        page_count,
        encrypted,
        pages,
    })
}

fn skipped_page(idx: usize) -> Page {
    Page {
        page: idx + 1,
        markdown: String::new(),
        tables: Vec::new(),
        likely_scanned: false,
        ms: 0,
        error: Some(PageError::TimedOut),
        tables_error: None,
    }
}

/// Extracts one page, timing it. Text and tables are extracted independently: a failure (error or
/// panic) in one is recorded on the [`Page`] without discarding the other.
fn process_page(doc: &PdfDocument, idx: usize, md: &ConversionOptions, opts: &Options) -> Page {
    let _span = tracing::info_span!("page", page = idx + 1).entered();
    let started = Instant::now();

    let (markdown, error) = match guarded(|| doc.to_markdown(idx, md)) {
        Ok(markdown) => (markdown, None),
        Err(e) => (String::new(), Some(e)),
    };
    let (tables, tables_error) = if opts.tables_json {
        match guarded(|| doc.extract_tables(idx)) {
            Ok(tables) => (tables.into_iter().map(Table::from).collect(), None),
            Err(e) => (Vec::new(), Some(e)),
        }
    } else {
        (Vec::new(), None)
    };
    // No text but an image on the page means a scan that needs OCR; a blank or graphics-only page does not.
    let likely_scanned = error.is_none()
        && markdown.trim().is_empty()
        && guarded(|| Ok::<_, std::convert::Infallible>(has_images(doc, idx))).unwrap_or(false);
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    if let Some(error) = error.as_ref().or(tables_error.as_ref()) {
        tracing::warn!(ms, error = %error, "page failed");
    } else {
        tracing::debug!(
            ms,
            tables = tables.len(),
            scanned = likely_scanned,
            "page done"
        );
    }
    Page {
        page: idx + 1,
        markdown,
        tables,
        likely_scanned,
        ms,
        error,
        tables_error,
    }
}

/// Runs `f`, turning both an error and a panic into a message.
fn guarded<T, E: ToString>(f: impl FnOnce() -> Result<T, E>) -> Result<T, PageError> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result.map_err(|e| PageError::Failed(e.to_string())),
        Err(_) => Err(PageError::Panicked),
    }
}

fn has_images(doc: &PdfDocument, idx: usize) -> bool {
    doc.page_image_handles(idx)
        .is_ok_and(|handles| !handles.is_empty())
}

/// High-water resident set size in MiB, from `/proc/self/status` (Linux only).
#[must_use]
pub fn peak_rss_mb() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kb| kb / 1024)
}
