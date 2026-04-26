use datacite_ror::extract::{run as extract_run, ExtractArgs};
use datacite_ror::query::{run as query_run, QueryArgs};
use datacite_ror::reconcile::{run as reconcile_run, ReconcileArgs};
use datacite_ror::{EnrichedRecord, ExistingAssignment, ResolutionSource};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::io::Write;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn three_stage_pipeline_end_to_end() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // ---- Set up mock match service ----
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "US National Science Foundation"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "message": {
                    "items": [{ "id": "https://ror.org/021nxhr62", "confidence": 0.8 }],
                    "target-data": "test",
                    "strategy": "funder-name-to-ror-search"
                }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "UnknownFunder"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "ok",
                "message": { "items": [], "target-data": "test", "strategy": "x" }
            })))
            .mount(&server)
            .await;

        // ---- Prepare inputs ----
        let input_dir = TempDir::new().unwrap();
        let work_dir = TempDir::new().unwrap();

        let sample_path = input_dir.path().join("part_0000.jsonl.gz");
        let f = File::create(&sample_path).unwrap();
        let mut gz = GzEncoder::new(f, Compression::default());
        for line in [
            // DOI 1: bare funder name matching via mock
            r#"{"id":"10.1/a","attributes":{"fundingReferences":[{"funderName":"US National Science Foundation"}]}}"#,
            // DOI 2: already-asserted ROR
            r#"{"id":"10.1/b","attributes":{"fundingReferences":[{"funderName":"Some Org","funderIdentifier":"https://ror.org/bbbbbbbbb","funderIdentifierType":"ROR"}]}}"#,
            // DOI 3: Crossref Funder ID → fundref mapping
            r#"{"id":"10.1/c","attributes":{"fundingReferences":[{"funderName":"NSF via Crossref","funderIdentifier":"100000001","funderIdentifierType":"Crossref Funder ID"}]}}"#,
            // DOI 4: unknown funder → will fail match
            r#"{"id":"10.1/d","attributes":{"fundingReferences":[{"funderName":"UnknownFunder"}]}}"#,
        ] {
            gz.write_all(line.as_bytes()).unwrap();
            gz.write_all(b"\n").unwrap();
        }
        gz.finish().unwrap();

        // Minimal ROR dump
        let ror_path = work_dir.path().join("ror.json");
        fs::write(&ror_path, r#"[
            {
                "id": "https://ror.org/021nxhr62",
                "names": [{"value": "NSF-canonical", "types": ["ror_display"]}],
                "external_ids": [{"type": "fundref", "all": ["100000001"], "preferred": "100000001"}]
            },
            {
                "id": "https://ror.org/bbbbbbbbb",
                "names": [{"value": "Some Org-canonical", "types": ["ror_display"]}]
            }
        ]"#).unwrap();

        // ---- Stage 1: extract ----
        tokio::task::spawn_blocking({
            let input = input_dir.path().to_path_buf();
            let output = work_dir.path().to_path_buf();
            move || {
                extract_run(ExtractArgs { input, output, threads: 1, batch_size: 10 }).unwrap();
            }
        }).await.unwrap();

        assert!(work_dir.path().join("doi_funders.jsonl").exists());
        assert!(work_dir.path().join("unique_funder_names.json").exists());

        // ---- Stage 2: query ----
        let uri = server.uri();
        tokio::task::spawn_blocking({
            let dir = work_dir.path().to_path_buf();
            move || {
                query_run(QueryArgs {
                    input: dir.clone(), output: dir,
                    base_url: uri, task: "funder".into(),
                    concurrency: 2, timeout: 5, resume: false,
                }).unwrap();
            }
        }).await.unwrap();

        let matches = fs::read_to_string(work_dir.path().join("ror_matches.jsonl")).unwrap();
        assert!(matches.contains("US National Science Foundation"));
        assert!(matches.contains("021nxhr62"));

        let failed = fs::read_to_string(work_dir.path().join("ror_matches.failed.jsonl")).unwrap();
        assert!(failed.contains("UnknownFunder"));

        // ---- Stage 3: reconcile ----
        tokio::task::spawn_blocking({
            let dir = work_dir.path().to_path_buf();
            let ror = ror_path.clone();
            move || {
                reconcile_run(ReconcileArgs {
                    input: dir.clone(),
                    output: Some(dir.join("enriched_records.jsonl")),
                    ror_data: ror,
                    enrichment_format: false, enrichment_config: None,
                }).unwrap();
            }
        }).await.unwrap();

        // ---- Assertions on reconcile outputs ----
        let enriched = fs::read_to_string(work_dir.path().join("enriched_records.jsonl")).unwrap();
        let enriched_records: Vec<EnrichedRecord> = enriched.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        // DOI 1 should be enriched; DOIs 2 & 3 routed to existing; DOI 4 has no match.
        assert!(enriched_records.iter().any(|r| r.doi == "10.1/a"));
        assert!(!enriched_records.iter().any(|r| r.doi == "10.1/b"));
        assert!(!enriched_records.iter().any(|r| r.doi == "10.1/c"));
        assert!(!enriched_records.iter().any(|r| r.doi == "10.1/d"));

        let existing = fs::read_to_string(work_dir.path().join("existing_assignments.jsonl")).unwrap();
        let existing_rows: Vec<ExistingAssignment> = existing.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(existing_rows.len(), 2);
        let asserted = existing_rows.iter().find(|r| r.doi == "10.1/b").unwrap();
        assert_eq!(asserted.resolution_source, ResolutionSource::Asserted);
        let fundref = existing_rows.iter().find(|r| r.doi == "10.1/c").unwrap();
        assert_eq!(fundref.resolution_source, ResolutionSource::FundrefMapping);
        assert_eq!(fundref.resolved_ror_id, "https://ror.org/021nxhr62");
    });
}
