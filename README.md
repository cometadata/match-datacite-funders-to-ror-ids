# match-datacite-funders-to-ror-ids

CLI tool to extract unique funder names from the DataCite public data file, match them against ROR IDs via a match service, and reconcile matches back to DOI/funder records.

## Installation

### Prerequisites

- [DataCite public data file](https://support.datacite.org/docs/datacite-public-data-file)
- A running match service exposing `GET /match?task=funder&input=<name>`
- [ROR data dump](https://ror.readme.io/docs/data-dump) (for name resolution and fundref→ROR cross-walks)

### Build

```bash
cargo build --release
```

## Usage

The tool provides three subcommands that form a pipeline:

1. `extract` — Extract unique funder names from DataCite JSONL.gz files
2. `query` — Match funder names against the match service
3. `reconcile` — Reconcile matches back to DOI/funder records

### Extract

```bash
datacite-ror extract --input <DIR> --output <DIR> [OPTIONS]
```

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Directory containing `.jsonl.gz` files | Required |
| `--output` | `-o` | Working directory for output files | Required |
| `--threads` | `-t` | Number of threads (0 = auto) | 0 |
| `--batch-size` | `-b` | Records per batch | 5000 |

**Outputs:**
- `unique_funder_names.json` — JSON array of unique funder name strings
- `doi_funders.jsonl` — one line per fundingReference with DOI, name, existing identifier, award metadata, and the raw source object

### Query

```bash
datacite-ror query --input <DIR> --output <DIR> [OPTIONS]
```

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Working directory (reads `unique_funder_names.json`) | Required |
| `--output` | `-o` | Working directory (writes match files) | Required |
| `--base-url` | `-u` | Match service base URL | `http://localhost:8000` |
| `--task` |  | Match-service task name | `funder` |
| `--concurrency` | `-c` | Concurrent requests | 50 |
| `--timeout` | `-t` | Request timeout in seconds | 30 |
| `--resume` | `-r` | Resume from checkpoint | false |

**Outputs:**
- `ror_matches.jsonl` — successful matches (includes `confidence`)
- `ror_matches.failed.jsonl` — failed queries (no match or errors)
- `ror_matches.checkpoint` — checkpoint file for resuming

### Reconcile

```bash
datacite-ror reconcile --input <DIR> --output <FILE> --ror-data <FILE> [OPTIONS]
```

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input` | `-i` | Working directory (reads relationship + match files) | Required |
| `--output` | `-o` | Output file path | `enriched_records.jsonl` or `enrichments.jsonl` |
| `--ror-data` | `-r` | Path to ROR data dump JSON file | Required |
| `--enrichment-format` |  | Emit DataCite enrichment format (per-funder records) | false |
| `--enrichment-config` |  | YAML config file (required with `--enrichment-format`) |  |

**Outputs:**

| File | Description |
|------|-------------|
| `enriched_records.jsonl` | DOIs enriched with ROR matches (default format, one record per DOI) |
| `enrichments.jsonl` | Per-funder enrichment records in [DataCite enrichment format](https://github.com/cometadata/datacite-enrichment) (when `--enrichment-format` is used) |
| `existing_assignments.jsonl` | Funders whose existing identifier already resolves to a ROR (asserted or fundref-mapped) |
| `existing_assignments_aggregated.jsonl` | Counts per `(funder_name, ror_id, resolution_source)` |
| `disagreements.jsonl` | User disagreements (same name → multiple ROR IDs) and match disagreements (our match differs from existing) |

## Intermediate file formats

### `doi_funders.jsonl`

```json
{
  "doi": "10.1234/example",
  "funding_ref_idx": 0,
  "funder_name": "National Science Foundation",
  "funder_name_hash": "a1b2c3d4e5f67890",
  "existing_identifier": "100000001",
  "existing_identifier_type": "Crossref Funder ID",
  "award_number": "AST-2001760",
  "award_title": "...",
  "original_funding_reference": { "...": "..." }
}
```

`existing_identifier` and `existing_identifier_type` are normalized: ROR → `https://ror.org/<id>`, Crossref Funder ID → bare digits. If the raw value unambiguously matches a scheme different from the stated type (e.g. a ROR URL labelled as "Crossref Funder ID"), the stated type is corrected.

### `ror_matches.jsonl`

```json
{
  "funder_name": "National Science Foundation",
  "funder_name_hash": "a1b2c3d4e5f67890",
  "ror_id": "https://ror.org/021nxhr62",
  "confidence": 0.4976
}
```

## Disagreement detection

Two forms:

- **User disagreement** — the same funder name has been labelled with multiple different ROR IDs across DOIs (asserted or cross-walked from Crossref Funder IDs).
- **Match disagreement** — the match-service result differs from an existing ROR (asserted or cross-walked). Emitted once per distinct `(existing_ror_id, resolution_source)` conflict.

## Full pipeline example

```bash
WORK_DIR=/work/datacite-funders

datacite-ror extract \
  --input /data/datacite/DataCite_Public_Data_File_2024 \
  --output $WORK_DIR \
  --threads 16

datacite-ror query \
  --input $WORK_DIR \
  --output $WORK_DIR \
  --base-url http://localhost:8000 \
  --concurrency 50 \
  --resume

datacite-ror reconcile \
  --input $WORK_DIR \
  --output $WORK_DIR/enriched_records.jsonl \
  --ror-data /data/ror/v2.6-2026-04-14-ror-data.json

# Or, DataCite enrichment format:
datacite-ror reconcile \
  --input $WORK_DIR \
  --ror-data /data/ror/v2.6-2026-04-14-ror-data.json \
  --enrichment-format \
  --enrichment-config enrichment_config.yaml
```

## Checkpointing

`query` supports checkpointing for long-running jobs:

- Progress saved to `ror_matches.checkpoint`
- `--resume` continues from where it stopped
- Checkpoint tracks processed funder names by hash
