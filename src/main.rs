use anyhow::Result;
use clap::{Parser, Subcommand};
use datacite_ror::{extract, query, reconcile};

#[derive(Parser)]
#[command(name = "datacite-ror")]
#[command(about = "Extract funder names from DataCite, match them, reconcile matches")]
#[command(version)]
#[command(propagate_version = true)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Extract unique funder names from DataCite files
    Extract(extract::ExtractArgs),
    /// Match funder names against the match service
    Query(query::QueryArgs),
    /// Reconcile matches back to DOI/funder records
    Reconcile(reconcile::ReconcileArgs),
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if cli.verbose {
        std::env::set_var("RUST_LOG", "debug");
    }

    match cli.command {
        Commands::Extract(args) => extract::run(args),
        Commands::Query(args) => query::run(args),
        Commands::Reconcile(args) => reconcile::run(args),
    }
}
