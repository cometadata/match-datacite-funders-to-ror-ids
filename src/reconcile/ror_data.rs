use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct RorName {
    value: String,
    types: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ExternalId {
    #[serde(rename = "type")]
    id_type: String,
    all: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RorRecord {
    id: String,
    names: Vec<RorName>,
    #[serde(default)]
    external_ids: Vec<ExternalId>,
}

/// Bundle of lookups derived from a single pass over the ROR dump.
pub struct RorLookup {
    pub ror_names: HashMap<String, String>,
    pub fundref_to_ror: HashMap<String, String>,
}

pub fn load_ror_data<P: AsRef<Path>>(path: P) -> Result<RorLookup> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);
    let records: Vec<RorRecord> = serde_json::from_reader(reader)?;

    let mut ror_names = HashMap::new();
    let mut fundref_to_ror = HashMap::new();

    for rec in records {
        let display = rec
            .names
            .iter()
            .find(|n| n.types.iter().any(|t| t == "ror_display"))
            .or_else(|| rec.names.first())
            .map(|n| n.value.clone());
        if let Some(name) = display {
            ror_names.insert(rec.id.clone(), name);
        }

        for eid in &rec.external_ids {
            if eid.id_type == "fundref" {
                for fundref in &eid.all {
                    fundref_to_ror.insert(fundref.clone(), rec.id.clone());
                }
            }
        }
    }

    Ok(RorLookup { ror_names, fundref_to_ror })
}
