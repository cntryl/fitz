mod benchmark_summary;
mod generate_inventory;
mod validate_tests;

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "fitz-tools")]
#[command(about = "Repository maintenance tools for Fitz")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    ValidateTests(ValidateTestsArgs),
    GenerateInventory(GenerateInventoryArgs),
    BenchmarkSummary(BenchmarkSummaryArgs),
}

#[derive(Debug, Args)]
struct ValidateTestsArgs {
    #[arg(long, short)]
    summary: bool,
    #[arg(long, short)]
    file: Option<PathBuf>,
    #[arg(long, short)]
    json: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct GenerateInventoryArgs {
    #[arg(long, default_value = ".")]
    root: PathBuf,
    #[arg(long, default_value = "inventory.md")]
    output: PathBuf,
    #[arg(long)]
    replace: bool,
}

#[derive(Debug, Args)]
struct BenchmarkSummaryArgs {
    #[arg(long, default_value = ".")]
    root: PathBuf,
}

fn main() {
    let cli = Cli::parse();
    let exit_code = match cli.command {
        Commands::ValidateTests(args) => validate_tests::run(args),
        Commands::GenerateInventory(args) => generate_inventory::run(args),
        Commands::BenchmarkSummary(args) => benchmark_summary::run(args),
    }
    .unwrap_or_else(|error| {
        eprintln!("error: {error:#}");
        1
    });

    std::process::exit(exit_code);
}
