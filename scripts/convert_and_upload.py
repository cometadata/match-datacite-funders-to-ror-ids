#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "pyarrow>=14.0.0",
#     "huggingface_hub>=0.19.0",
#     "orjson>=3.9.0",
#     "tqdm>=4.66.0",
# ]
# ///
"""
Convert DataCite-ROR matching output to Parquet and upload to HuggingFace.

This script:
1. Collects statistics from DataCite-ROR matching output files
2. Converts JSONL/JSON files to Parquet format (with sharding for large files)
3. Uploads to HuggingFace as a dataset with multiple configs
4. Generates a dataset card with statistics and documentation

Usage with uv:
    uv run convert_and_upload.py --stats-only
    uv run convert_and_upload.py --convert-only
    uv run convert_and_upload.py --token YOUR_HF_TOKEN
"""

import argparse
import os
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import HfApi, create_repo
from tqdm import tqdm


REPO_ID = "cometadata/datacite-affiliations-matched-ror"
ROR_VERSION = "v2.1-2026-01-15-ror-data"
ROR_DOI = "https://doi.org/10.5281/zenodo.18260365"
SOURCE_TOOL = "https://github.com/cometadata/match-datacite-affiliations-to-ror-ids"

TARGET_SHARD_SIZE = 1 * 1024 * 1024 * 1024  # 1 GB
BATCH_SIZE = 50_000


@dataclass
class FileConfig:
    filename: str
    config_name: str
    is_json_array: bool = False
    is_string_array: bool = False
    shard_large: bool = False


FILE_CONFIGS = [
    FileConfig("doi_author_affiliations.jsonl", "doi_author_affiliations", shard_large=True),
    FileConfig("enriched_records.jsonl", "enriched_records", shard_large=True),
    FileConfig("ror_matches.jsonl", "ror_matches"),
    FileConfig("ror_matches.failed.jsonl", "ror_matches_failed"),
    FileConfig("unique_affiliations.json", "unique_affiliations", is_json_array=True, is_string_array=True),
    FileConfig("existing_assignments.jsonl", "existing_assignments", shard_large=True),
    FileConfig("existing_assignments_aggregated.jsonl", "existing_assignments_aggregated"),
    FileConfig("disagreements.jsonl", "disagreements"),
]


def count_lines(filepath: Path) -> int:
    count = 0
    with open(filepath, "rb") as f:
        for _ in f:
            count += 1
    return count


def count_json_array(filepath: Path) -> int:
    with open(filepath, "rb") as f:
        data = orjson.loads(f.read())
    return len(data)


def get_file_size(filepath: Path) -> int:
    return filepath.stat().st_size


def format_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def iter_jsonl(filepath: Path) -> Iterator[dict]:
    with open(filepath, "rb") as f:
        for line_num, line in enumerate(f, 1):
            if line.strip():
                obj = orjson.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError(
                        f"{filepath}:{line_num}: Expected dict, got {type(obj).__name__}"
                    )
                yield obj


def iter_json_array(filepath: Path, wrap_strings: bool = False) -> Iterator[dict]:
    """If wrap_strings, string items become {"affiliation": item}."""
    with open(filepath, "rb") as f:
        data = orjson.loads(f.read())
    for idx, item in enumerate(data):
        if isinstance(item, dict):
            yield item
        elif wrap_strings and isinstance(item, str):
            yield {"affiliation": item}
        else:
            raise ValueError(
                f"{filepath}[{idx}]: Expected dict, got {type(item).__name__}"
            )


def collect_stats(input_dir: Path) -> dict:
    stats = {
        "files": {},
        "total_records": 0,
        "total_size_bytes": 0,
        "top_ror_ids": [],
        "error_distribution": {},
        "match_rate": 0.0,
        "existing_assignments": {
            "total_records": 0,
            "unique_affiliations": 0,
            "overlap_with_new_matches": 0,
            "agreement_count": 0,
            "agreement_rate": 0.0,
        },
        "disagreements": {
            "total_count": 0,
            "by_type": {},
            "disagreement_rate": 0.0,
            "top_patterns": [],
        },
    }

    print("Collecting statistics...")

    for config in FILE_CONFIGS:
        filepath = input_dir / config.filename
        if not filepath.exists():
            print(f"  Warning: {config.filename} not found, skipping")
            continue

        size = get_file_size(filepath)

        if config.is_json_array:
            record_count = count_json_array(filepath)
        else:
            record_count = count_lines(filepath)

        stats["files"][config.config_name] = {
            "filename": config.filename,
            "records": record_count,
            "size_bytes": size,
            "size_human": format_size(size),
        }
        stats["total_records"] += record_count
        stats["total_size_bytes"] += size

        print(f"  {config.filename}: {record_count:,} records ({format_size(size)})")

    if "ror_matches" in stats["files"] and "unique_affiliations" in stats["files"]:
        matched = stats["files"]["ror_matches"]["records"]
        total = stats["files"]["unique_affiliations"]["records"]
        if total > 0:
            stats["match_rate"] = matched / total
            print(f"\n  Match rate: {matched:,} / {total:,} = {stats['match_rate']:.2%}")

    ror_matches_path = input_dir / "ror_matches.jsonl"
    if ror_matches_path.exists():
        print("\n  Collecting top ROR IDs...")
        ror_counter: Counter = Counter()
        for record in tqdm(iter_jsonl(ror_matches_path), desc="  Scanning matches"):
            if "ror_id" in record:
                ror_counter[record["ror_id"]] += 1
        stats["top_ror_ids"] = ror_counter.most_common(20)
        print(f"  Found {len(ror_counter):,} unique ROR IDs")

    failed_path = input_dir / "ror_matches.failed.jsonl"
    if failed_path.exists():
        print("\n  Collecting error distribution...")
        error_counter: Counter = Counter()
        for record in tqdm(iter_jsonl(failed_path), desc="  Scanning errors"):
            error = record.get("error") or "unknown"
            if not isinstance(error, str):
                error = str(error)
            if len(error) > 100:
                error = error[:100] + "..."
            error_counter[error] += 1
        stats["error_distribution"] = dict(error_counter.most_common(10))

    stats["total_size_human"] = format_size(stats["total_size_bytes"])

    collect_existing_assignment_stats(input_dir, stats)
    collect_disagreement_stats(input_dir, stats)

    overlap = stats["existing_assignments"].get("overlap_with_new_matches", 0)
    disagreements = stats["disagreements"].get("total_count", 0)
    if overlap > 0:
        agreement_count = overlap - disagreements
        stats["existing_assignments"]["agreement_count"] = agreement_count
        stats["existing_assignments"]["agreement_rate"] = agreement_count / overlap
        print(f"\n  Agreement rate: {agreement_count:,} / {overlap:,} = {stats['existing_assignments']['agreement_rate']:.2%}")

    return stats


def collect_existing_assignment_stats(input_dir: Path, stats: dict) -> None:
    """Collect statistics about pre-existing ROR assignments."""
    agg_path = input_dir / "existing_assignments_aggregated.jsonl"
    if not agg_path.exists():
        print("\n  existing_assignments_aggregated.jsonl not found, skipping existing assignment stats")
        return

    print("\n  Collecting existing assignment statistics...")

    existing_hashes = set()
    total_records = 0

    for record in tqdm(iter_jsonl(agg_path), desc="  Scanning aggregated assignments"):
        existing_hashes.add(record.get("affiliation_hash"))
        total_records += record.get("count", 1)

    stats["existing_assignments"]["unique_affiliations"] = len(existing_hashes)
    stats["existing_assignments"]["total_records"] = total_records

    ror_matches_path = input_dir / "ror_matches.jsonl"
    if ror_matches_path.exists():
        overlap_count = 0
        for record in tqdm(iter_jsonl(ror_matches_path), desc="  Calculating overlap with new matches"):
            if record.get("affiliation_hash") in existing_hashes:
                overlap_count += 1
        stats["existing_assignments"]["overlap_with_new_matches"] = overlap_count
        print(f"  Overlap with new matches: {overlap_count:,}")

    print(f"  Unique affiliations with existing assignments: {len(existing_hashes):,}")
    print(f"  Total existing assignment records: {total_records:,}")


def collect_disagreement_stats(input_dir: Path, stats: dict) -> None:
    """Collect statistics about disagreements between new and existing assignments."""
    disagreements_path = input_dir / "disagreements.jsonl"
    if not disagreements_path.exists():
        print("\n  disagreements.jsonl not found, skipping disagreement stats")
        return

    print("\n  Collecting disagreement statistics...")

    type_counter: Counter = Counter()
    pattern_counter: Counter = Counter()

    for record in tqdm(iter_jsonl(disagreements_path), desc="  Scanning disagreements"):
        disagreement_type = record.get("type", "unknown")
        type_counter[disagreement_type] += 1

        if disagreement_type == "match":
            existing_id = record.get("existing_ror_id", "unknown")
            matched_id = record.get("matched_ror_id", "unknown")
            existing_name = record.get("existing_ror_name", "")
            matched_name = record.get("matched_ror_name", "")
            pattern_counter[(existing_id, existing_name, matched_id, matched_name)] += 1

    total_disagreements = sum(type_counter.values())
    stats["disagreements"]["total_count"] = total_disagreements
    stats["disagreements"]["by_type"] = dict(type_counter)

    stats["disagreements"]["top_patterns"] = [
        {
            "existing_ror_id": existing_id,
            "existing_ror_name": existing_name,
            "matched_ror_id": matched_id,
            "matched_ror_name": matched_name,
            "count": count,
        }
        for (existing_id, existing_name, matched_id, matched_name), count
        in pattern_counter.most_common(10)
    ]

    overlap = stats["existing_assignments"].get("overlap_with_new_matches", 0)
    if overlap > 0:
        stats["disagreements"]["disagreement_rate"] = total_disagreements / overlap

    print(f"  Total disagreements: {total_disagreements:,}")
    print(f"  By type: {dict(type_counter)}")
    if overlap > 0:
        print(f"  Disagreement rate: {total_disagreements:,} / {overlap:,} = {stats['disagreements']['disagreement_rate']:.2%}")


def infer_schema_from_sample(filepath: Path, config: FileConfig, sample_size: int = 1000) -> pa.Schema:
    if config.is_json_array:
        records = list(iter_json_array(filepath, wrap_strings=config.is_string_array))[:sample_size]
    else:
        records = []
        for i, record in enumerate(iter_jsonl(filepath)):
            if i >= sample_size:
                break
            records.append(record)

    if not records:
        raise ValueError(f"No records found in {filepath}")

    return pa.Table.from_pylist(records).schema


def convert_to_parquet(
    input_dir: Path,
    output_dir: Path,
    config: FileConfig,
    stats: dict,
) -> list[Path]:
    filepath = input_dir / config.filename
    if not filepath.exists():
        print(f"Warning: {config.filename} not found, skipping")
        return []

    config_output_dir = output_dir / "data" / config.config_name
    config_output_dir.mkdir(parents=True, exist_ok=True)

    file_size = get_file_size(filepath)
    record_count = stats["files"].get(config.config_name, {}).get("records", 0)

    if config.shard_large and file_size > TARGET_SHARD_SIZE:
        num_shards = max(1, int(file_size / TARGET_SHARD_SIZE) + 1)
        records_per_shard = record_count // num_shards + 1
    else:
        num_shards = 1
        records_per_shard = record_count

    print(f"\nConverting {config.filename} to {num_shards} shard(s)...")

    schema = infer_schema_from_sample(filepath, config)
    print(f"  Schema: {schema}")

    if config.is_json_array:
        record_iter = iter_json_array(filepath, wrap_strings=config.is_string_array)
    else:
        record_iter = iter_jsonl(filepath)

    output_files = []
    current_shard = 0
    batch = []
    records_in_shard = 0
    writer: pq.ParquetWriter | None = None

    def get_shard_path(shard_idx: int) -> Path:
        return config_output_dir / f"train-{shard_idx:05d}-of-{num_shards:05d}.parquet"

    def open_writer(shard_idx: int) -> pq.ParquetWriter:
        path = get_shard_path(shard_idx)
        return pq.ParquetWriter(path, schema, compression="snappy")

    def write_batch(w: pq.ParquetWriter, records: list[dict]) -> None:
        if records:
            table = pa.Table.from_pylist(records, schema=schema)
            w.write_table(table)

    pbar = tqdm(total=record_count, desc=f"  Processing")
    writer = open_writer(current_shard)

    for record in record_iter:
        batch.append(record)
        records_in_shard += 1
        pbar.update(1)

        if len(batch) >= BATCH_SIZE:
            write_batch(writer, batch)
            batch = []

        if num_shards > 1 and records_in_shard >= records_per_shard and current_shard < num_shards - 1:
            write_batch(writer, batch)
            batch = []
            writer.close()
            output_files.append(get_shard_path(current_shard))
            current_shard += 1
            records_in_shard = 0
            writer = open_writer(current_shard)

    write_batch(writer, batch)
    writer.close()
    output_files.append(get_shard_path(current_shard))
    pbar.close()

    total_rows = 0
    for f in output_files:
        if f.exists():
            total_rows += pq.read_metadata(f).num_rows

    print(f"  Written {total_rows:,} rows to {len(output_files)} file(s)")
    if total_rows != record_count:
        print(f"  WARNING: Row count mismatch! Expected {record_count:,}, got {total_rows:,}")

    return output_files


def generate_readme(stats: dict, output_dir: Path) -> Path:
    file_rows = []
    for config in FILE_CONFIGS:
        if config.config_name in stats["files"]:
            info = stats["files"][config.config_name]
            file_rows.append(
                f"| `{config.config_name}` | {info['records']:,} | {info['size_human']} |"
            )
    file_table = "\n".join(file_rows)

    ror_rows = []
    for ror_id, count in stats.get("top_ror_ids", [])[:20]:
        ror_rows.append(f"| {ror_id} | {count:,} |")
    ror_table = "\n".join(ror_rows) if ror_rows else "No data available"

    error_rows = []
    for error, count in stats.get("error_distribution", {}).items():
        error_escaped = error.replace("|", "\\|")
        error_rows.append(f"| {error_escaped} | {count:,} |")
    error_table = "\n".join(error_rows) if error_rows else "No data available"

    existing_stats = stats.get("existing_assignments", {})
    existing_total = existing_stats.get("total_records", 0)
    existing_unique = existing_stats.get("unique_affiliations", 0)
    existing_overlap = existing_stats.get("overlap_with_new_matches", 0)
    existing_agreement_rate = existing_stats.get("agreement_rate", 0.0)

    disagreement_stats = stats.get("disagreements", {})
    disagreement_total = disagreement_stats.get("total_count", 0)
    disagreement_rate = disagreement_stats.get("disagreement_rate", 0.0)
    disagreement_by_type = disagreement_stats.get("by_type", {})
    match_disagreements = disagreement_by_type.get("match", 0)
    user_disagreements = disagreement_by_type.get("user", 0)

    disagreement_pattern_rows = []
    for pattern in disagreement_stats.get("top_patterns", [])[:10]:
        existing_name = pattern.get("existing_ror_name", "")
        existing_id = pattern.get("existing_ror_id", "")
        matched_name = pattern.get("matched_ror_name", "")
        matched_id = pattern.get("matched_ror_id", "")
        count = pattern.get("count", 0)
        existing_cell = f"{existing_name} ({existing_id})" if existing_name else existing_id
        matched_cell = f"{matched_name} ({matched_id})" if matched_name else matched_id
        disagreement_pattern_rows.append(f"| {existing_cell} | {matched_cell} | {count:,} |")
    disagreement_pattern_table = "\n".join(disagreement_pattern_rows) if disagreement_pattern_rows else "No disagreements found"

    readme_content = f"""---
license: cc0-1.0
task_categories:
  - text-classification
language:
  - en
tags:
  - research
  - affiliations
  - ror
  - datacite
  - metadata
  - scholarly-infrastructure
pretty_name: DataCite Affiliations Matched to ROR
size_categories:
  - 100M<n<1B
configs:
  - config_name: doi_author_affiliations
    data_files:
      - split: train
        path: data/doi_author_affiliations/*.parquet
  - config_name: enriched_records
    data_files:
      - split: train
        path: data/enriched_records/*.parquet
  - config_name: ror_matches
    data_files:
      - split: train
        path: data/ror_matches/*.parquet
  - config_name: ror_matches_failed
    data_files:
      - split: train
        path: data/ror_matches_failed/*.parquet
  - config_name: unique_affiliations
    data_files:
      - split: train
        path: data/unique_affiliations/*.parquet
  - config_name: existing_assignments
    data_files:
      - split: train
        path: data/existing_assignments/*.parquet
  - config_name: existing_assignments_aggregated
    data_files:
      - split: train
        path: data/existing_assignments_aggregated/*.parquet
  - config_name: disagreements
    data_files:
      - split: train
        path: data/disagreements/*.parquet
---

# DataCite Affiliations Matched to ROR

This dataset contains author affiliation data extracted from DataCite metadata records, matched against the Research Organization Registry (ROR).

## Dataset Description

- **Source:** [DataCite Public Data File](https://datacite.org/)
- **ROR Version:** [{ROR_VERSION}]({ROR_DOI})
- **Processing Tool:** [{SOURCE_TOOL.split('/')[-1]}]({SOURCE_TOOL})
- **Total Records:** {stats['total_records']:,}
- **Total Size:** {stats['total_size_human']}
- **Match Rate:** {stats['match_rate']:.2%}

## Dataset Configurations

| Configuration | Records | Size |
|---------------|---------|------|
{file_table}

## Configuration Details

### `doi_author_affiliations`

Flattened author-affiliation pairs extracted from DataCite records. Each row represents one author-affiliation relationship.

**Schema:**
- `doi` (string): The DOI of the work
- `author_idx` (int): Index of the author within the work
- `author_name` (string): Name of the author
- `affiliation_idx` (int): Index of the affiliation for this author
- `affiliation` (string): Raw affiliation string
- `affiliation_hash` (string): MD5 hash of the normalized affiliation string

### `enriched_records`

Original DataCite records enriched with ROR IDs where matches were found.

**Schema:**
- `doi` (string): The DOI of the work
- `creators` (list): List of creator objects with nested affiliation data including matched ROR IDs

### `ror_matches`

Successful affiliation-to-ROR matches.

**Schema:**
- `affiliation` (string): Raw affiliation string
- `affiliation_hash` (string): MD5 hash of the normalized affiliation string
- `ror_id` (string): Matched ROR ID

### `ror_matches_failed`

Affiliations that could not be matched to a ROR ID.

**Schema:**
- `affiliation` (string): Raw affiliation string
- `affiliation_hash` (string): MD5 hash of the normalized affiliation string
- `error` (string): Reason for match failure

### `unique_affiliations`

List of all unique affiliation strings found in the dataset.

**Schema:**
- `affiliation` (string): Raw affiliation string

### `existing_assignments`

Pre-existing ROR assignments found in DataCite records. Each row represents one author-affiliation-ROR relationship that was already present in the source data.

**Schema:**
- `doi` (string): The DOI of the work
- `author_idx` (int): Index of the author within the work
- `author_name` (string): Name of the author
- `affiliation` (string): Raw affiliation string
- `ror_id` (string): Pre-existing ROR ID in the DataCite record
- `ror_name` (string): Name of the ROR organization

### `existing_assignments_aggregated`

Aggregated view of pre-existing ROR assignments, grouped by affiliation string and ROR ID.

**Schema:**
- `affiliation` (string): Raw affiliation string
- `affiliation_hash` (string): MD5 hash of the normalized affiliation string
- `ror_id` (string): Pre-existing ROR ID
- `ror_name` (string): Name of the ROR organization
- `count` (int): Number of occurrences of this affiliation-ROR pair

### `disagreements`

Cases where the newly matched ROR ID differs from a pre-existing ROR assignment, or where multiple conflicting ROR IDs exist.

**Schema (type="match"):**
- `type` (string): "match" - disagreement between new match and existing assignment
- `affiliation` (string): Raw affiliation string
- `affiliation_hash` (string): MD5 hash of the normalized affiliation string
- `existing_ror_id` (string): Pre-existing ROR ID in DataCite
- `existing_ror_name` (string): Name of existing ROR organization
- `existing_count` (int): Occurrences of this existing assignment
- `matched_ror_id` (string): Newly matched ROR ID
- `matched_ror_name` (string): Name of newly matched organization

**Schema (type="user"):**
- `type` (string): "user" - multiple conflicting user-submitted ROR IDs
- `affiliation` (string): Raw affiliation string
- `affiliation_hash` (string): MD5 hash of the normalized affiliation string
- `ror_ids` (list): List of conflicting ROR assignments with counts

## Statistics

### Top 20 Most Common Matched ROR IDs

| ROR ID | Count |
|--------|-------|
{ror_table}

### Error Distribution (Failed Matches)

| Error Type | Count |
|------------|-------|
{error_table}

### Existing Assignment Coverage

| Metric | Value |
|--------|-------|
| Total records with existing ROR assignments | {existing_total:,} |
| Unique affiliations with existing assignments | {existing_unique:,} |
| Overlap with new matches | {existing_overlap:,} |
| Agreement rate | {existing_agreement_rate:.2%} |

### Disagreement Analysis

| Metric | Value |
|--------|-------|
| Total disagreements | {disagreement_total:,} |
| Disagreement rate | {disagreement_rate:.2%} |
| Match-type disagreements | {match_disagreements:,} |
| User-type disagreements | {user_disagreements:,} |

### Top Disagreement Patterns

| Existing ROR | Matched ROR | Count |
|--------------|-------------|-------|
{disagreement_pattern_table}

## Usage

```python
from datasets import load_dataset

# Load successful ROR matches
matches = load_dataset("cometadata/datacite-affiliations-matched-ror", "ror_matches")

# Load author-affiliation pairs (large dataset, use streaming)
affiliations = load_dataset(
    "cometadata/datacite-affiliations-matched-ror",
    "doi_author_affiliations",
    streaming=True
)

# Iterate over records
for record in affiliations["train"]:
    print(record["doi"], record["affiliation"])
    break
```

## License

This dataset is released under the [CC0 1.0 Universal (Public Domain Dedication)](https://creativecommons.org/publicdomain/zero/1.0/) license.

## Citation

If you use this dataset, please cite:

```bibtex
@dataset{{datacite_affiliations_ror,
  title = {{DataCite Affiliations Matched to ROR}},
  author = {{cometadata}},
  year = {{2026}},
  publisher = {{Hugging Face}},
  url = {{https://huggingface.co/datasets/cometadata/datacite-affiliations-matched-ror}}
}}
```

## Acknowledgments

- [DataCite](https://datacite.org/) for providing the source metadata
- [ROR](https://ror.org/) for the Research Organization Registry
"""

    readme_path = output_dir / "README.md"
    readme_path.write_text(readme_content)
    print(f"\nGenerated dataset card: {readme_path}")
    return readme_path


def upload_to_hf(
    output_dir: Path,
    repo_id: str,
    private: bool = False,
    token: str | None = None,
) -> str:
    api = HfApi(token=token)

    try:
        create_repo(
            repo_id=repo_id,
            repo_type="dataset",
            private=private,
            token=token,
            exist_ok=True,
        )
        print(f"Repository {repo_id} ready")
    except Exception as e:
        print(f"Note: {e}")

    print(f"\nUploading files to {repo_id}...")

    readme_path = output_dir / "README.md"
    if readme_path.exists():
        api.upload_file(
            path_or_fileobj=str(readme_path),
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
            token=token,
        )
        print("  Uploaded README.md")

    data_dir = output_dir / "data"
    if data_dir.exists():
        for config_dir in sorted(data_dir.iterdir()):
            if config_dir.is_dir():
                parquet_files = sorted(config_dir.glob("*.parquet"))
                for pf in tqdm(parquet_files, desc=f"  Uploading {config_dir.name}"):
                    path_in_repo = f"data/{config_dir.name}/{pf.name}"
                    api.upload_file(
                        path_or_fileobj=str(pf),
                        path_in_repo=path_in_repo,
                        repo_id=repo_id,
                        repo_type="dataset",
                        token=token,
                    )

    repo_url = f"https://huggingface.co/datasets/{repo_id}"
    print(f"\nUpload complete: {repo_url}")
    return repo_url


def verify_conversion(output_dir: Path, stats: dict) -> bool:
    print("\nVerifying conversion...")
    all_ok = True

    data_dir = output_dir / "data"
    for config in FILE_CONFIGS:
        config_dir = data_dir / config.config_name
        if not config_dir.exists():
            continue

        expected = stats["files"].get(config.config_name, {}).get("records", 0)
        actual = 0

        for pf in config_dir.glob("*.parquet"):
            actual += pq.read_metadata(pf).num_rows

        status = "✓" if actual == expected else "✗"
        print(f"  {status} {config.config_name}: {actual:,} / {expected:,}")

        if actual != expected:
            all_ok = False

    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description="Convert DataCite-ROR output to Parquet and upload to HuggingFace"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("/Volumes/Untitled 2/datacite-ror-output"),
        help="Input directory containing source files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/Volumes/Untitled 2/datacite-ror-output/hf_upload"),
        help="Output directory for Parquet files",
    )
    parser.add_argument(
        "--repo-id",
        type=str,
        default=REPO_ID,
        help=f"HuggingFace repository ID (default: {REPO_ID})",
    )
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only collect and print statistics, don't convert or upload",
    )
    parser.add_argument(
        "--convert-only",
        action="store_true",
        help="Convert to Parquet but don't upload to HuggingFace",
    )
    parser.add_argument(
        "--upload-only",
        action="store_true",
        help="Upload existing Parquet files to HuggingFace (skip conversion)",
    )
    parser.add_argument(
        "--private",
        action="store_true",
        help="Make the HuggingFace repository private",
    )
    parser.add_argument(
        "--token",
        type=str,
        help="HuggingFace API token (or set HF_TOKEN env var)",
    )
    parser.add_argument(
        "--files",
        type=str,
        nargs="+",
        choices=[c.config_name for c in FILE_CONFIGS],
        help="Only process specific files (by config name)",
    )

    args = parser.parse_args()

    token = args.token or os.environ.get("HF_TOKEN")
    configs_to_process = FILE_CONFIGS
    if args.files:
        configs_to_process = [c for c in FILE_CONFIGS if c.config_name in args.files]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stats = collect_stats(args.input_dir)

    if args.stats_only:
        print("\nStats collection complete.")
        stats_path = args.output_dir / "stats.json"
        with open(stats_path, "wb") as f:
            f.write(orjson.dumps(stats, option=orjson.OPT_INDENT_2))
        print(f"Stats saved to: {stats_path}")
        return

    if not args.upload_only:
        print("\n" + "=" * 60)
        print("Converting to Parquet format...")
        print("=" * 60)

        for config in configs_to_process:
            convert_to_parquet(args.input_dir, args.output_dir, config, stats)

        generate_readme(stats, args.output_dir)

        if not verify_conversion(args.output_dir, stats):
            print("\nWARNING: Some files have mismatched record counts!")
            response = input("Continue with upload? (y/N): ")
            if response.lower() != "y":
                print("Aborting.")
                sys.exit(1)

    if not args.convert_only:
        if not token:
            print("\nNo HuggingFace token provided. Set HF_TOKEN env var or use --token.")
            print("Skipping upload.")
        else:
            print("\n" + "=" * 60)
            print("Uploading to HuggingFace...")
            print("=" * 60)

            upload_to_hf(
                output_dir=args.output_dir,
                repo_id=args.repo_id,
                private=args.private,
                token=token,
            )

    print("\nDone!")


if __name__ == "__main__":
    main()
