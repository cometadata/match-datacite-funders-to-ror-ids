use anyhow::{Context, Result};
use clap::Args;
use indicatif::{ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::{hash_funder_name, RorMatch, RorMatchFailed};

mod checkpoint;
mod client;
pub use checkpoint::Checkpoint;
pub use client::RorClient;

#[derive(Args)]
pub struct QueryArgs {
    /// Working directory (reads unique_funder_names.json)
    #[arg(short, long)]
    pub input: PathBuf,

    /// Working directory (writes ror_matches.jsonl)
    #[arg(short, long)]
    pub output: PathBuf,

    /// Match service base URL
    #[arg(short = 'u', long, default_value = "http://localhost:8000")]
    pub base_url: String,

    /// Task name for the match endpoint
    #[arg(long, default_value = "funder")]
    pub task: String,

    /// Concurrent requests
    #[arg(short, long, default_value = "50")]
    pub concurrency: usize,

    /// Request timeout in seconds
    #[arg(short, long, default_value = "30")]
    pub timeout: u64,

    /// Resume from checkpoint
    #[arg(short, long)]
    pub resume: bool,
}

pub fn run(args: QueryArgs) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_async(args))
}

pub async fn run_async(args: QueryArgs) -> Result<()> {
    fs::create_dir_all(&args.output).context("Failed to create output directory")?;

    let names_path = args.input.join("unique_funder_names.json");
    let names_file = File::open(&names_path)
        .with_context(|| format!("Failed to open {}", names_path.display()))?;
    let names: Vec<String> = serde_json::from_reader(names_file)
        .context("Failed to parse unique_funder_names.json")?;

    info!("Loaded {} funder names", names.len());

    let checkpoint_path = args.output.join("ror_matches.checkpoint");
    let checkpoint = if args.resume && checkpoint_path.exists() {
        Checkpoint::load(&checkpoint_path).context("Failed to load checkpoint")?
    } else {
        Checkpoint::new(&checkpoint_path)
    };

    let to_process: Vec<(String, String)> = names
        .into_iter()
        .map(|n| {
            let h = hash_funder_name(&n);
            (n, h)
        })
        .filter(|(_, h)| !checkpoint.is_processed(h))
        .collect();

    let total = to_process.len();
    let already = checkpoint.len();
    if already > 0 {
        info!("Resuming: {} already processed, {} remaining", already, total);
    }
    if total == 0 {
        info!("No funder names to process");
        return Ok(());
    }

    let matches_path = args.output.join("ror_matches.jsonl");
    let failed_path = args.output.join("ror_matches.failed.jsonl");

    let matches_file = if args.resume && matches_path.exists() {
        fs::OpenOptions::new().append(true).open(&matches_path)?
    } else {
        File::create(&matches_path)?
    };
    let failed_file = if args.resume && failed_path.exists() {
        fs::OpenOptions::new().append(true).open(&failed_path)?
    } else {
        File::create(&failed_path)?
    };

    let matches_writer = Arc::new(Mutex::new(BufWriter::new(matches_file)));
    let failed_writer = Arc::new(Mutex::new(BufWriter::new(failed_file)));
    let checkpoint = Arc::new(Mutex::new(checkpoint));

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let client = Arc::new(RorClient::new(args.base_url, args.concurrency, args.timeout));
    let task = args.task;
    let mut handles = Vec::with_capacity(total);

    for (name, hash) in to_process {
        let client = Arc::clone(&client);
        let matches_writer = Arc::clone(&matches_writer);
        let failed_writer = Arc::clone(&failed_writer);
        let checkpoint = Arc::clone(&checkpoint);
        let pb = pb.clone();
        let task = task.clone();

        handles.push(tokio::spawn(async move {
            match client.query_funder(&name, &task).await {
                Ok(Some((ror_id, confidence))) => {
                    let rec = RorMatch {
                        funder_name: name.clone(),
                        funder_name_hash: hash.clone(),
                        ror_id,
                        confidence,
                    };
                    let mut w = matches_writer.lock().await;
                    if let Err(e) = writeln!(w, "{}", serde_json::to_string(&rec).unwrap()) {
                        error!("Failed to write match: {}", e);
                    }
                }
                Ok(None) => {
                    let rec = RorMatchFailed {
                        funder_name: name.clone(),
                        funder_name_hash: hash.clone(),
                        error: "No match found".to_string(),
                    };
                    let mut w = failed_writer.lock().await;
                    if let Err(e) = writeln!(w, "{}", serde_json::to_string(&rec).unwrap()) {
                        error!("Failed to write failure: {}", e);
                    }
                }
                Err(e) => {
                    let rec = RorMatchFailed {
                        funder_name: name.clone(),
                        funder_name_hash: hash.clone(),
                        error: e.to_string(),
                    };
                    let mut w = failed_writer.lock().await;
                    if let Err(e) = writeln!(w, "{}", serde_json::to_string(&rec).unwrap()) {
                        error!("Failed to write failure: {}", e);
                    }
                }
            }
            let mut cp = checkpoint.lock().await;
            cp.mark_processed(&hash);
            pb.inc(1);
        }));
    }

    for h in handles {
        if let Err(e) = h.await {
            error!("Task failed: {}", e);
        }
    }

    pb.finish_with_message("Done");

    matches_writer.lock().await.flush().context("Failed to flush matches file")?;
    failed_writer.lock().await.flush().context("Failed to flush failed file")?;
    checkpoint.lock().await.save().context("Failed to save checkpoint")?;

    info!("Query complete");
    Ok(())
}
