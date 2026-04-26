use datacite_ror::extract::parse_funding_references;
use datacite_ror::{hash_funder_name, FundingRecord};
use serde_json::json;

fn single_doi(funding_refs: serde_json::Value) -> serde_json::Value {
    json!({
        "id": "10.1234/test",
        "attributes": { "doi": "10.1234/test", "fundingReferences": funding_refs }
    })
}

#[test]
fn returns_empty_when_no_funding_references() {
    let rec = json!({ "id": "10.1234/test", "attributes": {} });
    assert!(parse_funding_references(&rec).is_empty());
}

#[test]
fn returns_empty_when_funding_references_is_empty_array() {
    let rec = single_doi(json!([]));
    assert!(parse_funding_references(&rec).is_empty());
}

#[test]
fn skips_entries_with_missing_or_empty_funder_name() {
    let rec = single_doi(json!([
        { "funderName": "" },
        { "awardNumber": "ABC-123" },   // no funderName at all
        { "funderName": "Real Funder" },
    ]));
    let records = parse_funding_references(&rec);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].funder_name, "Real Funder");
    assert_eq!(records[0].funding_ref_idx, 2);  // index preserved from source array
}

#[test]
fn preserves_funder_name_hash() {
    let rec = single_doi(json!([{ "funderName": "NSF" }]));
    let records = parse_funding_references(&rec);
    assert_eq!(records[0].funder_name_hash, hash_funder_name("NSF"));
}

#[test]
fn extracts_doi_from_id_field_first() {
    let rec = json!({
        "id": "10.5555/from-id",
        "attributes": { "doi": "10.1234/different", "fundingReferences": [{ "funderName": "X" }] }
    });
    let records = parse_funding_references(&rec);
    assert_eq!(records[0].doi, "10.5555/from-id");
}

#[test]
fn falls_back_to_attributes_doi_when_id_missing() {
    let rec = json!({
        "attributes": { "doi": "10.1234/fallback", "fundingReferences": [{ "funderName": "X" }] }
    });
    let records = parse_funding_references(&rec);
    assert_eq!(records[0].doi, "10.1234/fallback");
}

#[test]
fn drops_record_when_no_doi_found() {
    let rec = json!({ "attributes": { "fundingReferences": [{ "funderName": "X" }] } });
    assert!(parse_funding_references(&rec).is_empty());
}

#[test]
fn preserves_award_fields() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "awardNumber": "ABC-123",
        "awardTitle": "Some grant title",
        "awardURI": "https://example.com/grant/ABC-123"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.award_number.as_deref(), Some("ABC-123"));
    assert_eq!(r.award_title.as_deref(), Some("Some grant title"));
    assert_eq!(r.award_uri.as_deref(), Some("https://example.com/grant/ABC-123"));
}

#[test]
fn preserves_original_funding_reference() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "021nxhr62",
        "funderIdentifierType": "ROR",
        "schemeUri": "https://ror.org",
        "awardNumber": "ABC-123",
        "weirdExtraKey": "value"
    }]));
    let r = &parse_funding_references(&rec)[0];
    let orig = r.original_funding_reference.as_ref().unwrap();
    assert_eq!(orig.get("weirdExtraKey").and_then(|v| v.as_str()), Some("value"));
    assert_eq!(orig.get("schemeUri").and_then(|v| v.as_str()), Some("https://ror.org"));
}

// ---------- Identifier normalization & type handling ----------

#[test]
fn normalizes_bare_ror_identifier() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "021nxhr62",
        "funderIdentifierType": "ROR"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("https://ror.org/021nxhr62"));
    assert_eq!(r.existing_identifier_type.as_deref(), Some("ROR"));
}

#[test]
fn normalizes_ror_url_identifier() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "https://ror.org/021nxhr62",
        "funderIdentifierType": "ROR"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("https://ror.org/021nxhr62"));
}

#[test]
fn normalizes_fundref_doi_url_identifier() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "https://doi.org/10.13039/100000001",
        "funderIdentifierType": "Crossref Funder ID"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("100000001"));
    assert_eq!(r.existing_identifier_type.as_deref(), Some("Crossref Funder ID"));
}

#[test]
fn overrides_mislabeled_ror_as_crossref() {
    // User labelled it "Crossref Funder ID" but value is a ROR URL.
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "https://ror.org/021nxhr62",
        "funderIdentifierType": "Crossref Funder ID"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("https://ror.org/021nxhr62"));
    assert_eq!(r.existing_identifier_type.as_deref(), Some("ROR"));
}

#[test]
fn overrides_mislabeled_crossref_as_ror() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "10.13039/100000001",
        "funderIdentifierType": "ROR"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("100000001"));
    assert_eq!(r.existing_identifier_type.as_deref(), Some("Crossref Funder ID"));
}

#[test]
fn keeps_raw_value_when_normalization_fails_for_known_type() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "not a valid id",
        "funderIdentifierType": "ROR"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("not a valid id"));
    assert_eq!(r.existing_identifier_type.as_deref(), Some("ROR"));
}

#[test]
fn passes_through_isni_identifier_unchanged() {
    let rec = single_doi(json!([{
        "funderName": "NSF",
        "funderIdentifier": "0000 0001 2345 6789",
        "funderIdentifierType": "ISNI"
    }]));
    let r = &parse_funding_references(&rec)[0];
    assert_eq!(r.existing_identifier.as_deref(), Some("0000 0001 2345 6789"));
    assert_eq!(r.existing_identifier_type.as_deref(), Some("ISNI"));
}

#[test]
fn no_identifier_when_funder_has_only_name() {
    let rec = single_doi(json!([{ "funderName": "NSF" }]));
    let r = &parse_funding_references(&rec)[0];
    assert!(r.existing_identifier.is_none());
    assert!(r.existing_identifier_type.is_none());
}

#[test]
fn emits_one_record_per_funding_reference() {
    let rec = single_doi(json!([
        { "funderName": "NSF", "funderIdentifier": "021nxhr62", "funderIdentifierType": "ROR" },
        { "funderName": "DOE" },
        { "funderName": "NIH", "funderIdentifier": "10.13039/100000002", "funderIdentifierType": "Crossref Funder ID" },
    ]));
    let records = parse_funding_references(&rec);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].funding_ref_idx, 0);
    assert_eq!(records[1].funding_ref_idx, 1);
    assert_eq!(records[2].funding_ref_idx, 2);
}

fn _type_check(_r: FundingRecord) {}

use datacite_ror::extract::{run, ExtractArgs};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;

fn write_gzipped_jsonl(path: &std::path::Path, lines: &[&str]) {
    let f = File::create(path).unwrap();
    let mut gz = GzEncoder::new(f, Compression::default());
    for l in lines {
        gz.write_all(l.as_bytes()).unwrap();
        gz.write_all(b"\n").unwrap();
    }
    gz.finish().unwrap();
}

#[test]
fn run_writes_doi_funders_and_unique_names() {
    let input = TempDir::new().unwrap();
    let output = TempDir::new().unwrap();
    let in_path = input.path().join("sample.jsonl.gz");
    write_gzipped_jsonl(
        &in_path,
        &[
            r#"{"id":"10.1/a","attributes":{"fundingReferences":[{"funderName":"NSF"},{"funderName":"DOE"}]}}"#,
            r#"{"id":"10.1/b","attributes":{"fundingReferences":[{"funderName":"NSF"}]}}"#,
            r#"{"id":"10.1/c","attributes":{}}"#,
        ],
    );

    run(ExtractArgs {
        input: input.path().to_path_buf(),
        output: output.path().to_path_buf(),
        threads: 1,
        batch_size: 10,
    })
    .unwrap();

    let jsonl = fs::read_to_string(output.path().join("doi_funders.jsonl")).unwrap();
    let lines: Vec<_> = jsonl.lines().collect();
    assert_eq!(lines.len(), 3);  // 2 for 10.1/a + 1 for 10.1/b; 10.1/c has no fundingReferences

    let names_file = fs::read_to_string(output.path().join("unique_funder_names.json")).unwrap();
    let names: Vec<String> = serde_json::from_str(&names_file).unwrap();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"NSF".to_string()));
    assert!(names.contains(&"DOE".to_string()));
}
