//! PDF -> structured JSON + Markdown, tuned for Lambda: one page is extracted,
//! serialised into the output and dropped before the next, and a failure (error
//! or panic) on one page is recorded instead of failing the whole document.

use pdf_oxide::converters::ConversionOptions;
use pdf_oxide::document::PdfDocument;
use pdf_oxide::structure::table_extractor as oxide;
use serde::Serialize;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

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
    pub page_count: usize,
    pub encrypted: bool,
    pub pages: Vec<Page>,
}

impl Extraction {
    #[must_use]
    pub fn pages_failed(&self) -> usize {
        self.pages.iter().filter(|p| p.error.is_some()).count()
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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

#[derive(Debug, Clone)]
pub struct Options {
    /// Also emit tables as cell grids. Table detection then runs twice per page
    /// (once for the Markdown, once here); turn off if only Markdown is needed.
    pub tables_json: bool,
    /// Password for encrypted documents.
    pub password: Option<String>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            tables_json: true,
            password: None,
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
        .map(|idx| process_page(&doc, idx, &md_opts, opts))
        .collect();

    Ok(Extraction {
        pdf_version: format!("{major}.{minor}"),
        page_count,
        encrypted,
        pages,
    })
}

/// Extracts one page, timing it and turning errors and panics into a failed [`Page`].
fn process_page(doc: &PdfDocument, idx: usize, md: &ConversionOptions, opts: &Options) -> Page {
    let _span = tracing::info_span!("page", page = idx + 1).entered();
    let started = Instant::now();
    let outcome = catch_unwind(AssertUnwindSafe(|| extract_page(doc, idx, md, opts)))
        .unwrap_or_else(|_| Err("panic during extraction".into()));
    let ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let page = match outcome {
        Ok((markdown, tables, likely_scanned)) => Page {
            page: idx + 1,
            likely_scanned,
            markdown,
            tables,
            ms,
            error: None,
        },
        Err(error) => Page {
            page: idx + 1,
            markdown: String::new(),
            tables: Vec::new(),
            likely_scanned: false,
            ms,
            error: Some(error),
        },
    };
    match &page.error {
        Some(error) => tracing::warn!(ms, error, "page failed"),
        None => tracing::debug!(
            ms,
            tables = page.tables.len(),
            scanned = page.likely_scanned,
            "page done"
        ),
    }
    page
}

fn extract_page(
    doc: &PdfDocument,
    idx: usize,
    md: &ConversionOptions,
    opts: &Options,
) -> Result<(String, Vec<Table>, bool), String> {
    let markdown = doc.to_markdown(idx, md).map_err(|e| e.to_string())?;
    // No text but an image on the page means a scan that needs OCR; a blank or graphics-only page does not.
    let likely_scanned = markdown.trim().is_empty() && has_images(doc, idx);
    let tables = if opts.tables_json {
        doc.extract_tables(idx)
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(Table::from)
            .collect()
    } else {
        Vec::new()
    };
    Ok((markdown, tables, likely_scanned))
}

fn has_images(doc: &PdfDocument, idx: usize) -> bool {
    doc.page_image_handles(idx).is_ok_and(|handles| !handles.is_empty())
}
