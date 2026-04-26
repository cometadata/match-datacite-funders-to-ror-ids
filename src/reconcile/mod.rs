mod ror_data;
pub use ror_data::{load_ror_data, RorLookup};

use anyhow::Result;
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::{
    Disagreement, EnrichedFundingReference, EnrichedRecord, EnrichmentConfig, EnrichmentOutputRecord, ExistingAssignment, ExistingAssignmentAggregated, FundingRecord, ResolutionSource, RorIdCount, RorMatch,
};

#[derive(Args)]
pub struct ReconcileArgs {
    #[arg(short, long)]
    pub input: PathBuf,

    #[arg(short, long)]
    pub output: Option<PathBuf>,

    #[arg(short, long)]
    pub ror_data: PathBuf,

    #[arg(long)]
    pub enrichment_format: bool,

    #[arg(long)]
    pub enrichment_config: Option<PathBuf>,
}

pub fn load_ror_matches<P: AsRef<Path>>(path: P) -> Result<HashMap<String, String>> {
    let mut lookup = HashMap::new();
    if !path.as_ref().exists() {
        return Ok(lookup);
    }
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(m) = serde_json::from_str::<RorMatch>(&line) {
            lookup.insert(m.funder_name_hash, m.ror_id);
        }
    }
    Ok(lookup)
}

pub fn run(args: ReconcileArgs) -> Result<()> {
    let relationships_path = args.input.join("doi_funders.jsonl");
    let matches_path = args.input.join("ror_matches.jsonl");

    let default_output = if args.enrichment_format {
        PathBuf::from("enrichments.jsonl")
    } else {
        PathBuf::from("enriched_records.jsonl")
    };
    let output_path = args.output.unwrap_or(default_output);
    let output_dir = output_path.parent().unwrap_or(Path::new("."));

    let enrichment_config = if args.enrichment_format {
        let config_path = args.enrichment_config.as_ref().ok_or_else(|| {
            anyhow::anyhow!("--enrichment-config is required when using --enrichment-format")
        })?;
        let s = std::fs::read_to_string(config_path)?;
        let c: EnrichmentConfig = serde_yaml::from_str(&s)?;
        Some(c)
    } else {
        None
    };

    eprintln!("Loading ROR data from {:?}...", args.ror_data);
    let ror_lookup = load_ror_data(&args.ror_data)?;
    eprintln!(
        "Loaded {} ROR organizations, {} fundref mappings",
        ror_lookup.ror_names.len(),
        ror_lookup.fundref_to_ror.len()
    );

    eprintln!("Loading ROR matches from {:?}...", matches_path);
    let match_lookup = load_ror_matches(&matches_path)?;
    eprintln!("Loaded {} ROR matches", match_lookup.len());

    let line_count = {
        let f = File::open(&relationships_path)?;
        BufReader::new(f).lines().count() as u64
    };
    let progress = ProgressBar::new(line_count);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"),
    );

    let enriched_file = File::create(&output_path)?;
    let mut enriched_writer = BufWriter::new(enriched_file);

    let existing_file = File::create(output_dir.join("existing_assignments.jsonl"))?;
    let mut existing_writer = BufWriter::new(existing_file);

    let input_file = File::open(&relationships_path)?;
    let reader = BufReader::new(input_file);
    let mut records_existing = 0u64;
    let mut records_enriched = 0u64;

    use std::collections::HashMap as StdHashMap;
    let mut existing_tally: StdHashMap<(String, String, ResolutionSource), (String, usize)> = StdHashMap::new();
    // key = (funder_name_hash, resolved_ror_id, source); value = (funder_name, count)

    let mut current_doi: Option<String> = None;
    let mut current_group: Vec<FundingRecord> = Vec::new();

    for line in reader.lines() {
        progress.inc(1);
        let line = line?;
        if line.trim().is_empty() { continue; }
        let rec: FundingRecord = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(_) => continue,
        };

        if let Some((resolved_id, source)) = resolve_existing(&rec, &ror_lookup) {
            let resolved_name = ror_lookup.ror_names.get(&resolved_id).cloned()
                .unwrap_or_else(|| "Unknown".to_string());
            let assignment = ExistingAssignment {
                doi: rec.doi.clone(),
                funding_ref_idx: rec.funding_ref_idx,
                funder_name: rec.funder_name.clone(),
                existing_identifier: rec.existing_identifier.clone(),
                existing_identifier_type: rec.existing_identifier_type.clone(),
                resolved_ror_id: resolved_id,
                resolved_ror_name: resolved_name,
                resolution_source: source,
            };
            writeln!(existing_writer, "{}", serde_json::to_string(&assignment)?)?;
            records_existing += 1;
            let key = (rec.funder_name_hash.clone(), assignment.resolved_ror_id.clone(), source);
            existing_tally
                .entry(key)
                .and_modify(|v| v.1 += 1)
                .or_insert((rec.funder_name.clone(), 1));
            continue;
        }

        // Enrichment path: accumulate by DOI.
        if current_doi.as_ref() != Some(&rec.doi) {
            if !current_group.is_empty() {
                if let Some(ref config) = enrichment_config {
                    let recs = process_doi_group_enrichment(current_doi.as_ref().unwrap(), &current_group, &match_lookup, config);
                    for r in &recs {
                        writeln!(enriched_writer, "{}", serde_json::to_string(r)?)?;
                    }
                    records_enriched += recs.len() as u64;
                } else if let Some(enriched) = process_doi_group(
                    current_doi.as_ref().unwrap(),
                    &current_group,
                    &match_lookup,
                ) {
                    writeln!(enriched_writer, "{}", serde_json::to_string(&enriched)?)?;
                    records_enriched += 1;
                }
                current_group.clear();
            }
            current_doi = Some(rec.doi.clone());
        }
        current_group.push(rec);
    }
    if !current_group.is_empty() {
        if let Some(ref config) = enrichment_config {
            let recs = process_doi_group_enrichment(current_doi.as_ref().unwrap(), &current_group, &match_lookup, config);
            for r in &recs {
                writeln!(enriched_writer, "{}", serde_json::to_string(r)?)?;
            }
            records_enriched += recs.len() as u64;
        } else if let Some(enriched) = process_doi_group(
            current_doi.as_ref().unwrap(),
            &current_group,
            &match_lookup,
        ) {
            writeln!(enriched_writer, "{}", serde_json::to_string(&enriched)?)?;
            records_enriched += 1;
        }
    }

    progress.finish_with_message("Processing complete");
    enriched_writer.flush()?;
    existing_writer.flush()?;

    let agg_file = File::create(output_dir.join("existing_assignments_aggregated.jsonl"))?;
    let mut agg_writer = BufWriter::new(agg_file);
    for ((hash, ror_id, source), (name, count)) in &existing_tally {
        let ror_name = ror_lookup.ror_names.get(ror_id).cloned().unwrap_or_else(|| "Unknown".to_string());
        let agg = ExistingAssignmentAggregated {
            funder_name: name.clone(),
            funder_name_hash: hash.clone(),
            ror_id: ror_id.clone(),
            ror_name,
            resolution_source: *source,
            count: *count,
        };
        writeln!(agg_writer, "{}", serde_json::to_string(&agg)?)?;
    }
    agg_writer.flush()?;

    let dis_file = File::create(output_dir.join("disagreements.jsonl"))?;
    let mut dis_writer = BufWriter::new(dis_file);
    let mut user_dis_count = 0u64;
    let mut match_dis_count = 0u64;

    // Group tally entries by funder_name_hash → Vec<RorIdCount>.
    let mut by_hash: StdHashMap<String, Vec<RorIdCount>> = StdHashMap::new();
    let mut names_by_hash: StdHashMap<String, String> = StdHashMap::new();
    for ((hash, ror_id, source), (name, count)) in &existing_tally {
        names_by_hash.entry(hash.clone()).or_insert_with(|| name.clone());
        let ror_name = ror_lookup.ror_names.get(ror_id).cloned().unwrap_or_else(|| "Unknown".to_string());
        by_hash.entry(hash.clone()).or_default().push(RorIdCount {
            ror_id: ror_id.clone(),
            ror_name,
            resolution_source: *source,
            count: *count,
        });
    }

    for (hash, entries) in &by_hash {
        let funder_name = names_by_hash.get(hash).cloned().unwrap_or_default();

        // User disagreement: same name → multiple DISTINCT ROR IDs.
        let distinct_rors: std::collections::HashSet<&String> =
            entries.iter().map(|e| &e.ror_id).collect();
        if distinct_rors.len() > 1 {
            let dis = Disagreement::User {
                funder_name: funder_name.clone(),
                funder_name_hash: hash.clone(),
                ror_ids: entries.clone(),
            };
            writeln!(dis_writer, "{}", serde_json::to_string(&dis)?)?;
            user_dis_count += 1;
        }

        // Match disagreement: one record per (existing_ror_id, source) ≠ matcher's ror.
        if let Some(matched_ror) = match_lookup.get(hash) {
            for e in entries {
                if &e.ror_id != matched_ror {
                    let matched_name = ror_lookup.ror_names.get(matched_ror).cloned().unwrap_or_else(|| "Unknown".to_string());
                    let dis = Disagreement::Match {
                        funder_name: funder_name.clone(),
                        funder_name_hash: hash.clone(),
                        existing_ror_id: e.ror_id.clone(),
                        existing_ror_name: e.ror_name.clone(),
                        existing_resolution_source: e.resolution_source,
                        existing_count: e.count,
                        matched_ror_id: matched_ror.clone(),
                        matched_ror_name: matched_name,
                    };
                    writeln!(dis_writer, "{}", serde_json::to_string(&dis)?)?;
                    match_dis_count += 1;
                }
            }
        }
    }
    dis_writer.flush()?;

    eprintln!(
        "  User disagreements: {}\n  Match disagreements: {}",
        user_dis_count, match_dis_count
    );

    eprintln!(
        "\nResults:\n  Enriched records: {}\n  Existing assignments: {}",
        records_enriched, records_existing
    );
    Ok(())
}

fn process_doi_group(
    doi: &str,
    records: &[FundingRecord],
    match_lookup: &HashMap<String, String>,
) -> Option<EnrichedRecord> {
    let refs: Vec<EnrichedFundingReference> = records
        .iter()
        .filter_map(|r| {
            let ror_id = match_lookup.get(&r.funder_name_hash)?.clone();
            Some(EnrichedFundingReference {
                funder_name: r.funder_name.clone(),
                funder_identifier: ror_id,
                funder_identifier_type: "ROR".to_string(),
                scheme_uri: "https://ror.org".to_string(),
                award_number: r.award_number.clone(),
                award_title: r.award_title.clone(),
                award_uri: r.award_uri.clone(),
            })
        })
        .collect();
    if refs.is_empty() {
        None
    } else {
        Some(EnrichedRecord {
            doi: doi.to_string(),
            funding_references: refs,
        })
    }
}

/// If this record has a resolvable existing ROR ID, returns it and the source.
fn resolve_existing(
    rec: &FundingRecord,
    ror_lookup: &RorLookup,
) -> Option<(String, ResolutionSource)> {
    let id = rec.existing_identifier.as_deref()?;
    let id_type = rec.existing_identifier_type.as_deref()?;
    match id_type {
        t if t.eq_ignore_ascii_case("ROR") => {
            // Only count as asserted if the normalized identifier is present in the dump
            // or is well-formed. We treat any ROR URL form as "asserted" regardless of
            // whether it's in the dump — unknown-name is allowed.
            Some((id.to_string(), ResolutionSource::Asserted))
        }
        t if t.eq_ignore_ascii_case("Crossref Funder ID") => ror_lookup
            .fundref_to_ror
            .get(id)
            .map(|r| (r.clone(), ResolutionSource::FundrefMapping)),
        _ => None,
    }
}

fn process_doi_group_enrichment(
    doi: &str,
    records: &[FundingRecord],
    match_lookup: &HashMap<String, String>,
    config: &EnrichmentConfig,
) -> Vec<EnrichmentOutputRecord> {
    records
        .iter()
        .filter_map(|r| {
            let ror_id = match_lookup.get(&r.funder_name_hash)?.clone();

            let original_value = r
                .original_funding_reference
                .clone()
                .unwrap_or_else(|| minimal_funding_ref_value(r));

            let mut enriched_value = original_value.clone();
            if let Some(obj) = enriched_value.as_object_mut() {
                obj.insert("funderIdentifier".to_string(), serde_json::json!(ror_id));
                obj.insert("funderIdentifierType".to_string(), serde_json::json!("ROR"));
                obj.insert("schemeUri".to_string(), serde_json::json!("https://ror.org"));
            }

            Some(EnrichmentOutputRecord {
                doi: doi.to_string(),
                contributors: config.contributors.clone(),
                resources: config.resources.clone(),
                field: "fundingReferences".to_string(),
                action: "updateChild".to_string(),
                original_value,
                enriched_value,
            })
        })
        .collect()
}

/// Fallback when the parser did not retain original_funding_reference.
fn minimal_funding_ref_value(r: &FundingRecord) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("funderName".to_string(), serde_json::json!(&r.funder_name));
    if let Some(ref v) = r.award_number { obj.insert("awardNumber".to_string(), serde_json::json!(v)); }
    if let Some(ref v) = r.award_title { obj.insert("awardTitle".to_string(), serde_json::json!(v)); }
    if let Some(ref v) = r.award_uri { obj.insert("awardURI".to_string(), serde_json::json!(v)); }
    serde_json::Value::Object(obj)
}
