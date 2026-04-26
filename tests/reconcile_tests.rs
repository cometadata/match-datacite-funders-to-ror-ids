use datacite_ror::reconcile::{load_ror_data, RorLookup};
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

fn write_ror_dump(contents: &str) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("ror.json");
    let mut f = File::create(&path).unwrap();
    f.write_all(contents.as_bytes()).unwrap();
    (dir, path)
}

#[test]
fn loads_display_name_for_each_record() {
    let (_d, p) = write_ror_dump(r#"[
        {
            "id": "https://ror.org/aaaaaaaaa",
            "names": [
                {"value": "Org A", "types": ["ror_display", "label"]},
                {"value": "A", "types": ["acronym"]}
            ]
        }
    ]"#);
    let lookup = load_ror_data(&p).unwrap();
    assert_eq!(lookup.ror_names.get("https://ror.org/aaaaaaaaa").unwrap(), "Org A");
}

#[test]
fn falls_back_to_first_name_without_ror_display() {
    let (_d, p) = write_ror_dump(r#"[
        { "id": "https://ror.org/bbbbbbbbb", "names": [{"value": "First", "types": ["alias"]}] }
    ]"#);
    let lookup = load_ror_data(&p).unwrap();
    assert_eq!(lookup.ror_names.get("https://ror.org/bbbbbbbbb").unwrap(), "First");
}

#[test]
fn builds_fundref_to_ror_lookup_from_all_ids() {
    let (_d, p) = write_ror_dump(r#"[
        {
            "id": "https://ror.org/aaaaaaaaa",
            "names": [{"value": "A", "types": ["ror_display"]}],
            "external_ids": [
                { "type": "fundref", "all": ["100000001", "100000002"], "preferred": "100000001" }
            ]
        },
        {
            "id": "https://ror.org/bbbbbbbbb",
            "names": [{"value": "B", "types": ["ror_display"]}],
            "external_ids": [
                { "type": "fundref", "all": ["200000001"], "preferred": null },
                { "type": "isni", "all": ["0000 0001 2345 6789"], "preferred": null }
            ]
        }
    ]"#);
    let lookup = load_ror_data(&p).unwrap();
    assert_eq!(lookup.fundref_to_ror.get("100000001").unwrap(), "https://ror.org/aaaaaaaaa");
    assert_eq!(lookup.fundref_to_ror.get("100000002").unwrap(), "https://ror.org/aaaaaaaaa");
    assert_eq!(lookup.fundref_to_ror.get("200000001").unwrap(), "https://ror.org/bbbbbbbbb");
    assert!(lookup.fundref_to_ror.get("0000 0001 2345 6789").is_none());
}

#[test]
fn handles_records_without_external_ids() {
    let (_d, p) = write_ror_dump(r#"[
        { "id": "https://ror.org/aaaaaaaaa", "names": [{"value": "A", "types": ["ror_display"]}] }
    ]"#);
    let lookup: RorLookup = load_ror_data(&p).unwrap();
    assert_eq!(lookup.ror_names.len(), 1);
    assert!(lookup.fundref_to_ror.is_empty());
}

use datacite_ror::reconcile::{run, ReconcileArgs};
use datacite_ror::{ExistingAssignment, FundingRecord, ResolutionSource, RorMatch};
use std::fs;

fn write_jsonl<T: serde::Serialize>(path: &std::path::Path, records: &[T]) {
    let mut out = String::new();
    for r in records {
        out.push_str(&serde_json::to_string(r).unwrap());
        out.push('\n');
    }
    fs::write(path, out).unwrap();
}

fn minimal_ror_dump(path: &std::path::Path) {
    let body = r#"[
        {
            "id": "https://ror.org/021nxhr62",
            "names": [{"value": "National Science Foundation", "types": ["ror_display"]}],
            "external_ids": [
                { "type": "fundref", "all": ["100000001"], "preferred": "100000001" }
            ]
        },
        {
            "id": "https://ror.org/bbbbbbbbb",
            "names": [{"value": "Other Org", "types": ["ror_display"]}]
        }
    ]"#;
    fs::write(path, body).unwrap();
}

#[test]
fn routes_asserted_ror_to_existing_assignments() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(),
            funding_ref_idx: 0,
            funder_name: "NSF".into(),
            funder_name_hash: datacite_ror::hash_funder_name("NSF"),
            existing_identifier: Some("https://ror.org/021nxhr62".into()),
            existing_identifier_type: Some("ROR".into()),
            award_number: None,
            award_title: None,
            award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json");
    minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    })
    .unwrap();

    let existing = fs::read_to_string(work.join("existing_assignments.jsonl")).unwrap();
    let lines: Vec<&str> = existing.lines().collect();
    assert_eq!(lines.len(), 1);
    let parsed: ExistingAssignment = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed.doi, "10.1/a");
    assert_eq!(parsed.resolved_ror_id, "https://ror.org/021nxhr62");
    assert_eq!(parsed.resolved_ror_name, "National Science Foundation");
    assert_eq!(parsed.resolution_source, ResolutionSource::Asserted);
}

#[test]
fn routes_crossref_to_existing_via_fundref_mapping() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/b".into(),
            funding_ref_idx: 0,
            funder_name: "NSF".into(),
            funder_name_hash: datacite_ror::hash_funder_name("NSF"),
            existing_identifier: Some("100000001".into()),
            existing_identifier_type: Some("Crossref Funder ID".into()),
            award_number: None,
            award_title: None,
            award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json");
    minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let line = fs::read_to_string(work.join("existing_assignments.jsonl")).unwrap();
    let parsed: ExistingAssignment = serde_json::from_str(line.lines().next().unwrap()).unwrap();
    assert_eq!(parsed.resolved_ror_id, "https://ror.org/021nxhr62");
    assert_eq!(parsed.resolution_source, ResolutionSource::FundrefMapping);
}

#[test]
fn crossref_without_mapping_is_not_in_existing_assignments() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/c".into(),
            funding_ref_idx: 0,
            funder_name: "Mystery".into(),
            funder_name_hash: datacite_ror::hash_funder_name("Mystery"),
            existing_identifier: Some("999999999".into()),
            existing_identifier_type: Some("Crossref Funder ID".into()),
            award_number: None,
            award_title: None,
            award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json");
    minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let existing = fs::read_to_string(work.join("existing_assignments.jsonl")).unwrap();
    assert!(existing.is_empty(), "expected empty, got {:?}", existing);
}

use datacite_ror::EnrichedRecord;

#[test]
fn writes_enriched_record_for_matched_funder() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let hash_nsf = datacite_ror::hash_funder_name("NSF");

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(),
            funding_ref_idx: 0,
            funder_name: "NSF".into(),
            funder_name_hash: hash_nsf.clone(),
            existing_identifier: None,
            existing_identifier_type: None,
            award_number: Some("AST-2001760".into()),
            award_title: Some("Grant title".into()),
            award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl(
        &work.join("ror_matches.jsonl"),
        &[RorMatch {
            funder_name: "NSF".into(),
            funder_name_hash: hash_nsf,
            ror_id: "https://ror.org/021nxhr62".into(),
            confidence: 0.9,
        }],
    );
    let ror_path = work.join("ror.json");
    minimal_ror_dump(&ror_path);
    let output = work.join("enriched_records.jsonl");

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(output.clone()),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(&output).unwrap();
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 1);
    let parsed: EnrichedRecord = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed.doi, "10.1/a");
    assert_eq!(parsed.funding_references.len(), 1);
    assert_eq!(parsed.funding_references[0].funder_identifier, "https://ror.org/021nxhr62");
    assert_eq!(parsed.funding_references[0].funder_identifier_type, "ROR");
    assert_eq!(parsed.funding_references[0].scheme_uri, "https://ror.org");
    assert_eq!(parsed.funding_references[0].award_number.as_deref(), Some("AST-2001760"));
    assert_eq!(parsed.funding_references[0].award_title.as_deref(), Some("Grant title"));
}

#[test]
fn skips_doi_with_no_matches() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let hash = datacite_ror::hash_funder_name("Unmatched");

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/x".into(),
            funding_ref_idx: 0,
            funder_name: "Unmatched".into(),
            funder_name_hash: hash,
            existing_identifier: None,
            existing_identifier_type: None,
            award_number: None, award_title: None, award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json");
    minimal_ror_dump(&ror_path);
    let output = work.join("enriched_records.jsonl");

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(output.clone()),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(&output).unwrap();
    assert!(body.is_empty(), "expected empty, got {:?}", body);
}

#[test]
fn groups_multiple_funders_per_doi_into_one_record() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h_nsf = datacite_ror::hash_funder_name("NSF");
    let h_doe = datacite_ror::hash_funder_name("DOE");

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[
            FundingRecord {
                doi: "10.1/a".into(), funding_ref_idx: 0,
                funder_name: "NSF".into(), funder_name_hash: h_nsf.clone(),
                existing_identifier: None, existing_identifier_type: None,
                award_number: None, award_title: None, award_uri: None,
                original_funding_reference: None,
            },
            FundingRecord {
                doi: "10.1/a".into(), funding_ref_idx: 1,
                funder_name: "DOE".into(), funder_name_hash: h_doe.clone(),
                existing_identifier: None, existing_identifier_type: None,
                award_number: None, award_title: None, award_uri: None,
                original_funding_reference: None,
            },
        ],
    );
    write_jsonl(
        &work.join("ror_matches.jsonl"),
        &[
            RorMatch {
                funder_name: "NSF".into(), funder_name_hash: h_nsf,
                ror_id: "https://ror.org/021nxhr62".into(), confidence: 0.9,
            },
            RorMatch {
                funder_name: "DOE".into(), funder_name_hash: h_doe,
                ror_id: "https://ror.org/bbbbbbbbb".into(), confidence: 0.8,
            },
        ],
    );
    let ror_path = work.join("ror.json");
    minimal_ror_dump(&ror_path);
    let output = work.join("enriched_records.jsonl");

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(output.clone()),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(&output).unwrap();
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 1);
    let parsed: EnrichedRecord = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(parsed.funding_references.len(), 2);
}

use datacite_ror::{EnrichmentOutputRecord, EnrichmentConfig, EnrichmentContributor, EnrichmentResource};

fn write_config(path: &std::path::Path) {
    fs::write(path, r#"
contributors:
  - name: "TEST"
    contributorType: "Producer"
resources:
  - relatedIdentifier: "https://example.com/data"
    relatedIdentifierType: "URL"
    relationType: "IsDerivedFrom"
"#).unwrap();
}

#[test]
fn enrichment_format_emits_one_record_per_matched_funder() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h_nsf = datacite_ror::hash_funder_name("NSF");

    let orig = serde_json::json!({
        "funderName": "NSF",
        "awardNumber": "ABC-123",
        "awardTitle": "Grant"
    });

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(), funding_ref_idx: 0,
            funder_name: "NSF".into(), funder_name_hash: h_nsf.clone(),
            existing_identifier: None, existing_identifier_type: None,
            award_number: Some("ABC-123".into()),
            award_title: Some("Grant".into()),
            award_uri: None,
            original_funding_reference: Some(orig.clone()),
        }],
    );
    write_jsonl(
        &work.join("ror_matches.jsonl"),
        &[RorMatch {
            funder_name: "NSF".into(), funder_name_hash: h_nsf,
            ror_id: "https://ror.org/021nxhr62".into(), confidence: 0.9,
        }],
    );
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);
    let config_path = work.join("config.yaml"); write_config(&config_path);
    let output = work.join("enrichments.jsonl");

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(output.clone()),
        ror_data: ror_path,
        enrichment_format: true,
        enrichment_config: Some(config_path),
    }).unwrap();

    let body = fs::read_to_string(&output).unwrap();
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 1);
    let r: EnrichmentOutputRecord = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(r.doi, "10.1/a");
    assert_eq!(r.field, "fundingReferences");
    assert_eq!(r.action, "updateChild");
    assert_eq!(r.contributors.len(), 1);
    assert_eq!(r.resources.len(), 1);
    assert_eq!(r.original_value, orig);

    // enrichedValue should clone original and add the three ROR fields.
    let ev = &r.enriched_value;
    assert_eq!(ev.get("funderName").and_then(|v| v.as_str()), Some("NSF"));
    assert_eq!(ev.get("awardNumber").and_then(|v| v.as_str()), Some("ABC-123"));
    assert_eq!(ev.get("awardTitle").and_then(|v| v.as_str()), Some("Grant"));
    assert_eq!(ev.get("funderIdentifier").and_then(|v| v.as_str()), Some("https://ror.org/021nxhr62"));
    assert_eq!(ev.get("funderIdentifierType").and_then(|v| v.as_str()), Some("ROR"));
    assert_eq!(ev.get("schemeUri").and_then(|v| v.as_str()), Some("https://ror.org"));
}

#[test]
fn enrichment_format_requires_config() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    write_jsonl::<FundingRecord>(&work.join("doi_funders.jsonl"), &[]);
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);

    let err = run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enrichments.jsonl")),
        ror_data: ror_path,
        enrichment_format: true,
        enrichment_config: None,
    });
    assert!(err.is_err());
    let msg = format!("{}", err.unwrap_err());
    assert!(msg.contains("enrichment-config is required"));
}

#[test]
fn enrichment_format_preserves_unknown_keys_in_original() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h = datacite_ror::hash_funder_name("X");
    let orig = serde_json::json!({
        "funderName": "X",
        "schemeUri": "https://example.com",   // will be OVERWRITTEN in enriched
        "weirdKey": "preserved",
        "awardNumber": "1"
    });
    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(), funding_ref_idx: 0,
            funder_name: "X".into(), funder_name_hash: h.clone(),
            existing_identifier: None, existing_identifier_type: None,
            award_number: Some("1".into()), award_title: None, award_uri: None,
            original_funding_reference: Some(orig.clone()),
        }],
    );
    write_jsonl(&work.join("ror_matches.jsonl"), &[RorMatch {
        funder_name: "X".into(), funder_name_hash: h,
        ror_id: "https://ror.org/021nxhr62".into(), confidence: 0.5,
    }]);
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);
    let config = work.join("c.yaml"); write_config(&config);
    let output = work.join("enrichments.jsonl");

    run(ReconcileArgs {
        input: work.to_path_buf(), output: Some(output.clone()),
        ror_data: ror_path, enrichment_format: true,
        enrichment_config: Some(config),
    }).unwrap();

    let body = fs::read_to_string(&output).unwrap();
    let r: EnrichmentOutputRecord = serde_json::from_str(body.lines().next().unwrap()).unwrap();
    assert_eq!(r.original_value.get("weirdKey").and_then(|v| v.as_str()), Some("preserved"));
    assert_eq!(r.original_value.get("schemeUri").and_then(|v| v.as_str()), Some("https://example.com"));
    assert_eq!(r.enriched_value.get("weirdKey").and_then(|v| v.as_str()), Some("preserved"));
    assert_eq!(r.enriched_value.get("schemeUri").and_then(|v| v.as_str()), Some("https://ror.org"));  // overwritten
}

use datacite_ror::ExistingAssignmentAggregated;

#[test]
fn aggregates_existing_assignments_by_name_ror_and_source() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h = datacite_ror::hash_funder_name("NSF");

    let asserted = |doi: &str| FundingRecord {
        doi: doi.into(), funding_ref_idx: 0,
        funder_name: "NSF".into(), funder_name_hash: h.clone(),
        existing_identifier: Some("https://ror.org/021nxhr62".into()),
        existing_identifier_type: Some("ROR".into()),
        award_number: None, award_title: None, award_uri: None,
        original_funding_reference: None,
    };
    let via_fundref = |doi: &str| FundingRecord {
        doi: doi.into(), funding_ref_idx: 0,
        funder_name: "NSF".into(), funder_name_hash: h.clone(),
        existing_identifier: Some("100000001".into()),
        existing_identifier_type: Some("Crossref Funder ID".into()),
        award_number: None, award_title: None, award_uri: None,
        original_funding_reference: None,
    };

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[asserted("10.1/a"), asserted("10.1/b"), asserted("10.1/c"), via_fundref("10.1/d")],
    );
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(work.join("existing_assignments_aggregated.jsonl")).unwrap();
    let mut rows: Vec<ExistingAssignmentAggregated> = body
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    rows.sort_by(|a, b| a.resolution_source.cmp(&b.resolution_source));

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].resolution_source, datacite_ror::ResolutionSource::Asserted);
    assert_eq!(rows[0].count, 3);
    assert_eq!(rows[1].resolution_source, datacite_ror::ResolutionSource::FundrefMapping);
    assert_eq!(rows[1].count, 1);
}

use datacite_ror::{Disagreement, RorIdCount};

#[test]
fn user_disagreement_when_name_maps_to_multiple_rors() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h = datacite_ror::hash_funder_name("Ambiguous");

    let make = |doi: &str, ror: &str| FundingRecord {
        doi: doi.into(), funding_ref_idx: 0,
        funder_name: "Ambiguous".into(), funder_name_hash: h.clone(),
        existing_identifier: Some(ror.into()),
        existing_identifier_type: Some("ROR".into()),
        award_number: None, award_title: None, award_uri: None,
        original_funding_reference: None,
    };

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[make("10.1/a", "https://ror.org/021nxhr62"),
          make("10.1/b", "https://ror.org/bbbbbbbbb")],
    );
    write_jsonl::<RorMatch>(&work.join("ror_matches.jsonl"), &[]);
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path,
        enrichment_format: false,
        enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(work.join("disagreements.jsonl")).unwrap();
    let disagreements: Vec<Disagreement> = body.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let user_dis: Vec<_> = disagreements.iter().filter(|d| matches!(d, Disagreement::User { .. })).collect();
    assert_eq!(user_dis.len(), 1);
    if let Disagreement::User { ror_ids, .. } = user_dis[0] {
        assert_eq!(ror_ids.len(), 2);
    }
}

#[test]
fn match_disagreement_when_matcher_differs_from_asserted() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h = datacite_ror::hash_funder_name("NSF");

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(), funding_ref_idx: 0,
            funder_name: "NSF".into(), funder_name_hash: h.clone(),
            existing_identifier: Some("https://ror.org/bbbbbbbbb".into()),
            existing_identifier_type: Some("ROR".into()),
            award_number: None, award_title: None, award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl(
        &work.join("ror_matches.jsonl"),
        &[RorMatch {
            funder_name: "NSF".into(), funder_name_hash: h,
            ror_id: "https://ror.org/021nxhr62".into(), confidence: 0.9,
        }],
    );
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path, enrichment_format: false, enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(work.join("disagreements.jsonl")).unwrap();
    let match_dis: Vec<Disagreement> = body.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .filter(|d| matches!(d, Disagreement::Match { .. }))
        .collect();
    assert_eq!(match_dis.len(), 1);
    if let Disagreement::Match { existing_ror_id, matched_ror_id, existing_resolution_source, .. } = &match_dis[0] {
        assert_eq!(existing_ror_id, "https://ror.org/bbbbbbbbb");
        assert_eq!(matched_ror_id, "https://ror.org/021nxhr62");
        assert_eq!(*existing_resolution_source, datacite_ror::ResolutionSource::Asserted);
    }
}

#[test]
fn match_disagreement_when_matcher_differs_from_fundref_mapping() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h = datacite_ror::hash_funder_name("NSF");

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(), funding_ref_idx: 0,
            funder_name: "NSF".into(), funder_name_hash: h.clone(),
            existing_identifier: Some("100000001".into()),       // → maps to 021nxhr62 in our fixture
            existing_identifier_type: Some("Crossref Funder ID".into()),
            award_number: None, award_title: None, award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl(
        &work.join("ror_matches.jsonl"),
        &[RorMatch {
            funder_name: "NSF".into(), funder_name_hash: h,
            ror_id: "https://ror.org/bbbbbbbbb".into(),  // differs from fundref mapping
            confidence: 0.9,
        }],
    );
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path, enrichment_format: false, enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(work.join("disagreements.jsonl")).unwrap();
    let match_dis: Vec<Disagreement> = body.lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .filter(|d| matches!(d, Disagreement::Match { .. }))
        .collect();
    assert_eq!(match_dis.len(), 1);
    if let Disagreement::Match { existing_resolution_source, .. } = match_dis[0] {
        assert_eq!(existing_resolution_source, datacite_ror::ResolutionSource::FundrefMapping);
    }
}

#[test]
fn no_disagreement_when_matcher_agrees_with_existing() {
    let dir = TempDir::new().unwrap();
    let work = dir.path();
    let h = datacite_ror::hash_funder_name("NSF");

    write_jsonl(
        &work.join("doi_funders.jsonl"),
        &[FundingRecord {
            doi: "10.1/a".into(), funding_ref_idx: 0,
            funder_name: "NSF".into(), funder_name_hash: h.clone(),
            existing_identifier: Some("https://ror.org/021nxhr62".into()),
            existing_identifier_type: Some("ROR".into()),
            award_number: None, award_title: None, award_uri: None,
            original_funding_reference: None,
        }],
    );
    write_jsonl(
        &work.join("ror_matches.jsonl"),
        &[RorMatch {
            funder_name: "NSF".into(), funder_name_hash: h,
            ror_id: "https://ror.org/021nxhr62".into(), confidence: 0.9,
        }],
    );
    let ror_path = work.join("ror.json"); minimal_ror_dump(&ror_path);

    run(ReconcileArgs {
        input: work.to_path_buf(),
        output: Some(work.join("enriched_records.jsonl")),
        ror_data: ror_path, enrichment_format: false, enrichment_config: None,
    }).unwrap();

    let body = fs::read_to_string(work.join("disagreements.jsonl")).unwrap();
    assert!(body.is_empty(), "expected empty, got {:?}", body);
}
