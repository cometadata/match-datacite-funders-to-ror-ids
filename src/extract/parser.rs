use crate::identifiers::{normalize_fundref, normalize_ror, sniff_identifier, IdentifierScheme};
use crate::{hash_funder_name, FundingRecord};
use serde_json::Value;

pub fn parse_funding_references(record: &Value) -> Vec<FundingRecord> {
    let doi = match extract_doi(record) {
        Some(d) => d,
        None => return Vec::new(),
    };

    let refs = match record.pointer("/attributes/fundingReferences") {
        Some(Value::Array(arr)) => arr,
        _ => return Vec::new(),
    };

    let mut out = Vec::with_capacity(refs.len());
    for (idx, entry) in refs.iter().enumerate() {
        let funder_name = match entry.get("funderName").and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => continue,
        };

        let (existing_identifier, existing_identifier_type) =
            resolve_identifier(entry);

        out.push(FundingRecord {
            doi: doi.clone(),
            funding_ref_idx: idx,
            funder_name_hash: hash_funder_name(&funder_name),
            funder_name,
            existing_identifier,
            existing_identifier_type,
            award_number: entry.get("awardNumber").and_then(Value::as_str).map(String::from),
            award_title: entry.get("awardTitle").and_then(Value::as_str).map(String::from),
            award_uri: entry.get("awardURI").and_then(Value::as_str).map(String::from),
            original_funding_reference: Some(entry.clone()),
        });
    }
    out
}

fn extract_doi(record: &Value) -> Option<String> {
    record
        .get("id")
        .and_then(Value::as_str)
        .map(String::from)
        .or_else(|| {
            record
                .pointer("/attributes/doi")
                .and_then(Value::as_str)
                .map(String::from)
        })
}

/// Applies the mislabel-override policy: sniff the value; if it unambiguously
/// matches a scheme, use that scheme and the canonical form regardless of the
/// stated type. Otherwise normalize within the stated type; if that fails, keep
/// the raw value with the stated type.
fn resolve_identifier(entry: &Value) -> (Option<String>, Option<String>) {
    let raw = entry.get("funderIdentifier").and_then(Value::as_str);
    let stated_type = entry.get("funderIdentifierType").and_then(Value::as_str);

    let raw = match raw {
        Some(s) if !s.trim().is_empty() => s,
        _ => return (None, None),
    };

    // 1. Value-first sniff — overrides label mismatches.
    if let Some((scheme, canonical)) = sniff_identifier(raw) {
        let type_str = match scheme {
            IdentifierScheme::Ror => "ROR",
            IdentifierScheme::Fundref => "Crossref Funder ID",
        };
        return (Some(canonical), Some(type_str.to_string()));
    }

    // 2. Fall back to normalizing within the stated type.
    match stated_type {
        Some(t) if t.eq_ignore_ascii_case("ROR") => match normalize_ror(raw) {
            Some(c) => (Some(c), Some("ROR".to_string())),
            None => (Some(raw.to_string()), Some(t.to_string())),
        },
        Some(t) if t.eq_ignore_ascii_case("Crossref Funder ID") => match normalize_fundref(raw) {
            Some(c) => (Some(c), Some("Crossref Funder ID".to_string())),
            None => (Some(raw.to_string()), Some(t.to_string())),
        },
        // Unknown / absent type — pass through as-is.
        Some(t) => (Some(raw.to_string()), Some(t.to_string())),
        None => (Some(raw.to_string()), None),
    }
}
