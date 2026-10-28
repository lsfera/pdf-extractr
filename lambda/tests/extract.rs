use pdf_extract_lambda::{ExtractError, Options, extract};

const FIXTURE: &[u8] = include_bytes!("../../../pdf_oxide/tests/fixtures/1.pdf");

#[test]
fn extracts_every_page() {
    let e = extract(FIXTURE.to_vec(), &Options::default()).unwrap();
    assert_eq!(e.pages.len(), e.page_count);
    assert_eq!(e.pages_failed(), 0);
    assert_eq!(e.pages_scanned(), 0);
    assert!(e.pages.iter().enumerate().all(|(i, p)| p.page == i + 1));
    assert!(!e.markdown().is_empty());
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
fn text_pages_are_not_counted_as_scanned() {
    let extraction = extract(FIXTURE.to_vec(), &Options::default()).expect("fixture opens");
    assert_eq!(extraction.pages_scanned(), 0);
}
