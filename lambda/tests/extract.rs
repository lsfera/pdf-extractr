use pdf_extract_lambda::{ExtractError, Options, extract};
use std::time::Instant;

const FIXTURE: &[u8] = include_bytes!("fixtures/1.pdf");

#[test]
fn extracts_every_page() {
    let e = extract(FIXTURE.to_vec(), &Options::default()).unwrap();
    assert_eq!(e.pages().len(), e.page_count());
    assert_eq!(e.pages_failed(), 0);
    assert_eq!(e.pages_scanned(), 0);
    assert!(e.pages().iter().enumerate().all(|(i, p)| p.page == i + 1));
    assert_ne!(e.markdown(), "");
}

#[test]
fn tables_json_can_be_disabled() {
    let opts = Options {
        tables_json: false,
        ..Options::default()
    };
    let e = extract(FIXTURE.to_vec(), &opts).unwrap();
    assert_eq!(e.table_count(), 0);
}

#[test]
fn garbage_is_an_open_error() {
    let err = extract(b"not a pdf".to_vec(), &Options::default()).unwrap_err();
    assert!(matches!(err, ExtractError::Open(_)));
}

#[test]
fn password_is_redacted_in_debug() {
    let opts = Options {
        password: Some("hunter2".into()),
        ..Options::default()
    };
    assert!(!format!("{opts:?}").contains("hunter2"));
}

#[test]
fn an_expired_deadline_skips_pages_but_still_returns_a_document() {
    let opts = Options {
        deadline: Some(Instant::now()),
        ..Options::default()
    };
    let e = extract(FIXTURE.to_vec(), &opts).unwrap();
    assert_eq!(e.pages_failed(), e.page_count());
    assert!(e.pages().iter().all(|p| p.markdown.is_empty()));
    assert_eq!(
        e.pages()[0].error.as_ref().unwrap().to_string(),
        "skipped: the document's time budget ran out"
    );
}
