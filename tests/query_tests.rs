use datacite_ror::query::RorClient;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn ok_body(items: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "status": "ok",
        "message-version": "1.0.0",
        "message-type": "matched-item-list",
        "message": {
            "items": items,
            "target-data": "v2.6-test",
            "strategy": "funder-name-to-ror-search"
        }
    })
}

#[tokio::test]
async fn returns_id_and_confidence_on_match() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/match"))
        .and(query_param("task", "funder"))
        .and(query_param("input", "US National Science Foundation"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([
            { "id": "https://ror.org/021nxhr62", "confidence": 0.4976, "strategies": ["funder-name-to-ror-search"] }
        ]))))
        .mount(&server)
        .await;

    let client = RorClient::new(server.uri(), 1, 10);
    let result = client.query_funder("US National Science Foundation", "funder").await.unwrap();
    assert_eq!(result, Some(("https://ror.org/021nxhr62".to_string(), 0.4976)));
}

#[tokio::test]
async fn returns_none_on_empty_items() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([]))))
        .mount(&server)
        .await;

    let client = RorClient::new(server.uri(), 1, 10);
    let result = client.query_funder("unknown funder", "funder").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn picks_first_when_multiple_items() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([
            { "id": "https://ror.org/aaaaaaaaa", "confidence": 0.9 },
            { "id": "https://ror.org/bbbbbbbbb", "confidence": 0.5 }
        ]))))
        .mount(&server)
        .await;

    let client = RorClient::new(server.uri(), 1, 10);
    let (id, _) = client.query_funder("x", "funder").await.unwrap().unwrap();
    assert_eq!(id, "https://ror.org/aaaaaaaaa");
}

#[tokio::test]
async fn retries_on_429_with_retry_after() {
    let server = MockServer::start().await;

    // First call: 429 with Retry-After: 0 so the test doesn't wait
    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(
            ResponseTemplate::new(429)
                .insert_header("Retry-After", "0"),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Second call: 200 OK
    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([
            { "id": "https://ror.org/021nxhr62", "confidence": 0.7 }
        ]))))
        .mount(&server)
        .await;

    let client = RorClient::new(server.uri(), 1, 10);
    let result = client.query_funder("x", "funder").await.unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn returns_err_on_5xx() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/match"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let client = RorClient::new(server.uri(), 1, 10);
    let result = client.query_funder("x", "funder").await;
    assert!(result.is_err(), "expected error, got {:?}", result);
}

#[tokio::test]
async fn url_encodes_funder_name() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/match"))
        .and(query_param("task", "funder"))
        .and(query_param("input", "Name with spaces & ampersand"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([]))))
        .mount(&server)
        .await;

    let client = RorClient::new(server.uri(), 1, 10);
    client.query_funder("Name with spaces & ampersand", "funder").await.unwrap();
}

use datacite_ror::query::{run, QueryArgs};
use datacite_ror::RorMatch;
use std::fs;
use tempfile::TempDir;

#[test]
fn run_end_to_end_writes_matches_and_failures() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "NSF"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([
                { "id": "https://ror.org/021nxhr62", "confidence": 0.9 }
            ]))))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/match"))
            .and(query_param("input", "UnknownFunder"))
            .respond_with(ResponseTemplate::new(200).set_body_json(ok_body(serde_json::json!([]))))
            .mount(&server)
            .await;

        let work = TempDir::new().unwrap();
        fs::write(
            work.path().join("unique_funder_names.json"),
            serde_json::to_string(&vec!["NSF", "UnknownFunder"]).unwrap(),
        )
        .unwrap();

        let base_url = server.uri();
        let out_path = work.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            run(QueryArgs {
                input: out_path.clone(),
                output: out_path,
                base_url,
                task: "funder".to_string(),
                concurrency: 2,
                timeout: 5,
                resume: false,
            })
            .unwrap();
        })
        .await
        .unwrap();

        let matches = fs::read_to_string(work.path().join("ror_matches.jsonl")).unwrap();
        let failed = fs::read_to_string(work.path().join("ror_matches.failed.jsonl")).unwrap();

        let match_lines: Vec<&str> = matches.lines().collect();
        assert_eq!(match_lines.len(), 1);
        let parsed: RorMatch = serde_json::from_str(match_lines[0]).unwrap();
        assert_eq!(parsed.funder_name, "NSF");
        assert_eq!(parsed.ror_id, "https://ror.org/021nxhr62");
        assert_eq!(parsed.confidence, 0.9);

        assert_eq!(failed.lines().count(), 1);
        assert!(failed.contains("UnknownFunder"));
        assert!(failed.contains("No match found"));
    });
}

use datacite_ror::query::Checkpoint;

#[test]
fn checkpoint_save_load_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cp");
    let mut cp = Checkpoint::new(&path);
    cp.mark_processed("abc");
    cp.mark_processed("def");
    cp.save().unwrap();

    let loaded = Checkpoint::load(&path).unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded.is_processed("abc"));
    assert!(loaded.is_processed("def"));
    assert!(!loaded.is_processed("ghi"));
}
