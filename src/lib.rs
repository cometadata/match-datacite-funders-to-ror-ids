use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

pub mod identifiers;
pub mod extract;
pub mod query;
pub mod reconcile;

pub fn hash_funder_name(name: &str) -> String {
    format!("{:016x}", xxh3_64(name.as_bytes()))
}

/// One row per DataCite fundingReference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRecord {
    pub doi: String,
    pub funding_ref_idx: usize,
    pub funder_name: String,
    pub funder_name_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_identifier_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub award_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub award_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub award_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_funding_reference: Option<serde_json::Value>,
}

/// Successful match written to ror_matches.jsonl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorMatch {
    pub funder_name: String,
    pub funder_name_hash: String,
    pub ror_id: String,
    pub confidence: f64,
}

/// Failed match written to ror_matches.failed.jsonl.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorMatchFailed {
    pub funder_name: String,
    pub funder_name_hash: String,
    pub error: String,
}

/// One row per funder whose existing identifier resolved to a ROR
/// (either directly asserted or via fundref_to_ror lookup).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExistingAssignment {
    pub doi: String,
    pub funding_ref_idx: usize,
    pub funder_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_identifier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_identifier_type: Option<String>,
    pub resolved_ror_id: String,
    pub resolved_ror_name: String,
    pub resolution_source: ResolutionSource,
}

/// How an existing funder identifier was mapped to a ROR ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionSource {
    /// funderIdentifierType == "ROR" and identifier was parseable.
    Asserted,
    /// funderIdentifierType == "Crossref Funder ID" and we found a fundref→ROR mapping.
    FundrefMapping,
}

/// Aggregated per (funder_name_hash, ror_id, resolution_source).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExistingAssignmentAggregated {
    pub funder_name: String,
    pub funder_name_hash: String,
    pub ror_id: String,
    pub ror_name: String,
    pub resolution_source: ResolutionSource,
    pub count: usize,
}

/// One row inside a User disagreement's ror_ids array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RorIdCount {
    pub ror_id: String,
    pub ror_name: String,
    pub resolution_source: ResolutionSource,
    pub count: usize,
}

// ---------- Enrichment-format output (DataCite spec) ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichmentContributor {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name_type: Option<String>,
    pub contributor_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichmentResource {
    pub related_identifier: String,
    pub related_identifier_type: String,
    pub relation_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type_general: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnrichmentConfig {
    pub contributors: Vec<EnrichmentContributor>,
    pub resources: Vec<EnrichmentResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichmentOutputRecord {
    pub doi: String,
    pub contributors: Vec<EnrichmentContributor>,
    pub resources: Vec<EnrichmentResource>,
    pub field: String,
    pub action: String,
    pub original_value: serde_json::Value,
    pub enriched_value: serde_json::Value,
}

// ---------- Default enriched_records.jsonl ----------

/// One row per DOI with at least one matcher-matched funder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichedFundingReference {
    pub funder_name: String,
    pub funder_identifier: String,
    pub funder_identifier_type: String,   // always "ROR"
    pub scheme_uri: String,                // always "https://ror.org"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub award_number: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub award_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub award_uri: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrichedRecord {
    pub doi: String,
    pub funding_references: Vec<EnrichedFundingReference>,
}

// ---------- Disagreements ----------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Disagreement {
    User {
        funder_name: String,
        funder_name_hash: String,
        ror_ids: Vec<RorIdCount>,
    },
    Match {
        funder_name: String,
        funder_name_hash: String,
        existing_ror_id: String,
        existing_ror_name: String,
        existing_resolution_source: ResolutionSource,
        existing_count: usize,
        matched_ror_id: String,
        matched_ror_name: String,
    },
}
