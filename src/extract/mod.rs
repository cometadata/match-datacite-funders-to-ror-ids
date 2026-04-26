use anyhow::{Context, Result};
use clap::Args;
use crossbeam_channel::bounded;
use flate2::read::GzDecoder;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::{error, info};

use crate::FundingRecord;

mod parser;
pub use parser::parse_funding_references;

#[derive(Args)]
pub struct ExtractArgs {
    /// Directory containing .jsonl.gz files
    #[arg(short, long)]
    pub input: PathBuf,

    /// Working directory for output files
    #[arg(short, long)]
    pub output: PathBuf,

    /// Number of threads (0 = auto)
    #[arg(short, long, default_value = "0")]
    pub threads: usize,

    /// Records per batch
    #[arg(short, long, default_value = "5000")]
    pub batch_size: usize,
}

pub fn find_jsonl_gz_files<P: AsRef<Path>>(directory: P) -> Result<Vec<PathBuf>> {
    let pattern = directory.as_ref().join("**/*.jsonl.gz");
    let pattern_str = pattern.to_string_lossy();
    Ok(glob(&pattern_str)?.filter_map(Result::ok).collect())
}

fn process_file(
    filepath: &Path,
    unique_names: &Mutex<HashSet<String>>,
    tx: &crossbeam_channel::Sender<Vec<FundingRecord>>,
    batch_size: usize,
) -> Result<()> {
    let file = File::open(filepath)
        .with_context(|| format!("Failed to open {}", filepath.display()))?;
    let decoder = GzDecoder::new(file);
    let reader = BufReader::new(decoder);

    let mut batch = Vec::with_capacity(batch_size);

    for line in reader.lines() {
        let line_str = line?;
        if line_str.trim().is_empty() {
            continue;
        }

        if let Ok(record) = serde_json::from_str::<serde_json::Value>(&line_str) {
            let records = parse_funding_references(&record);

            if !records.is_empty() {
                let mut unique = unique_names.lock().unwrap();
                for r in &records {
                    unique.insert(r.funder_name.clone());
                }
            }

            batch.extend(records);

            if batch.len() >= batch_size && tx.send(std::mem::take(&mut batch)).is_err() {
                break;
            }
        }
    }

    if !batch.is_empty() {
        let _ = tx.send(batch);
    }

    Ok(())
}

pub fn run(args: ExtractArgs) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("datacite_ror=info".parse().unwrap()),
        )
        .try_init()
        .ok();

    fs::create_dir_all(&args.output)?;

    let num_threads = if args.threads > 0 { args.threads } else { num_cpus::get() };
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok();
    info!("Using {} threads", num_threads);

    let files = find_jsonl_gz_files(&args.input)?;
    info!("Found {} files to process", files.len());
    if files.is_empty() {
        return Ok(());
    }

    let progress = ProgressBar::new(files.len() as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40}] {pos}/{len} ({eta})")?
            .progress_chars("#>-"),
    );

    let unique_names: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let (tx, rx) = bounded::<Vec<FundingRecord>>(num_threads * 4);

    let output_path = args.output.join("doi_funders.jsonl");
    let writer_handle = std::thread::spawn(move || -> Result<()> {
        let file = File::create(&output_path)?;
        let mut writer = BufWriter::new(file);
        while let Ok(batch) = rx.recv() {
            for record in batch {
                serde_json::to_writer(&mut writer, &record)?;
                writer.write_all(b"\n")?;
            }
        }
        writer.flush()?;
        Ok(())
    });

    let unique_ref = Arc::clone(&unique_names);
    files.par_iter().for_each_with(tx.clone(), |tx, filepath| {
        if let Err(e) = process_file(filepath, &unique_ref, tx, args.batch_size) {
            error!("Error processing {}: {}", filepath.display(), e);
        }
        progress.inc(1);
    });

    drop(tx);
    writer_handle.join().unwrap()?;
    progress.finish();

    let unique = unique_names.lock().unwrap();
    let names_vec: Vec<&String> = unique.iter().collect();
    let names_path = args.output.join("unique_funder_names.json");
    let f = File::create(&names_path)?;
    serde_json::to_writer(f, &names_vec)?;

    info!("Extracted {} unique funder names", names_vec.len());
    info!("Output: {}", args.output.display());

    Ok(())
}
