use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};

use anyhow::{bail, Context, Result};

pub mod generate_messages;

const USAGE: &str = "usage: protocol_codegen --schema-dir <directory> --output-dir <directory>";

#[derive(Debug, PartialEq, Eq)]
struct CliArgs {
    schema_dir: PathBuf,
    output_dir: PathBuf,
}

fn main() {
    let arguments = env::args().skip(1).collect();
    let args = match parse_args(arguments) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("error: {error}\n\n{USAGE}");
            process::exit(2);
        }
    };

    if let Err(error) = run(args) {
        eprintln!("error: {error:#}");
        process::exit(1);
    }
}

fn parse_args(arguments: Vec<String>) -> std::result::Result<CliArgs, String> {
    let mut schema_dir = None;
    let mut output_dir = None;
    let mut arguments = arguments.into_iter();

    while let Some(argument) = arguments.next() {
        let target = match argument.as_str() {
            "--schema-dir" => &mut schema_dir,
            "--output-dir" => &mut output_dir,
            _ => return Err(format!("unknown argument: {argument}")),
        };

        if target.is_some() {
            return Err(format!("duplicate argument: {argument}"));
        }

        let value = arguments
            .next()
            .filter(|value| !value.starts_with("--"))
            .ok_or_else(|| format!("missing value for argument: {argument}"))?;
        *target = Some(PathBuf::from(value));
    }

    Ok(CliArgs {
        schema_dir: schema_dir
            .ok_or_else(|| "missing required argument: --schema-dir".to_owned())?,
        output_dir: output_dir
            .ok_or_else(|| "missing required argument: --output-dir".to_owned())?,
    })
}

fn run(args: CliArgs) -> Result<()> {
    if !args.output_dir.is_dir() {
        bail!(
            "output directory does not exist or is not a directory: {}",
            args.output_dir.display()
        );
    }

    clear_generated_messages(&args.output_dir)?;
    let input_file_paths = schema_paths(&args.schema_dir)?;
    generate_messages::run(&args.output_dir, input_file_paths)?;
    format_generated_files(&args.output_dir)?;

    Ok(())
}

fn clear_generated_messages(output_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(output_dir)
        .with_context(|| format!("failed to read output directory {}", output_dir.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.path().extension() == Some(OsStr::new("rs")) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn schema_paths(schema_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(schema_dir)
        .with_context(|| format!("failed to read schema directory {}", schema_dir.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.path().extension() == Some(OsStr::new("json")) {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn format_generated_files(output_dir: &Path) -> Result<()> {
    let mut paths = vec![output_dir.with_extension("rs")];
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_file() && entry.path().extension() == Some(OsStr::new("rs")) {
            paths.push(entry.path());
        }
    }
    paths.sort();

    let status = Command::new("rustfmt")
        .args(&paths)
        .status()
        .context("failed to run rustfmt")?;
    if !status.success() {
        bail!("rustfmt exited with status {status}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::parse_args;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn accepts_exact_schema_and_output_arguments() {
        let parsed =
            parse_args(args(&["--schema-dir", "schema", "--output-dir", "output"])).unwrap();

        assert_eq!(parsed.schema_dir, Path::new("schema"));
        assert_eq!(parsed.output_dir, Path::new("output"));
    }

    #[test]
    fn rejects_missing_arguments() {
        let error = parse_args(args(&["--schema-dir", "schema"])).unwrap_err();

        assert_eq!(error, "missing required argument: --output-dir");
    }

    #[test]
    fn rejects_duplicate_arguments() {
        let error = parse_args(args(&[
            "--schema-dir",
            "one",
            "--schema-dir",
            "two",
            "--output-dir",
            "output",
        ]))
        .unwrap_err();

        assert_eq!(error, "duplicate argument: --schema-dir");
    }

    #[test]
    fn rejects_unknown_arguments() {
        let error = parse_args(args(&[
            "--schema-dir",
            "schema",
            "--output-dir",
            "output",
            "--offline",
        ]))
        .unwrap_err();

        assert_eq!(error, "unknown argument: --offline");
    }

    #[test]
    fn rejects_positional_arguments() {
        let error = parse_args(args(&[
            "--schema-dir",
            "schema",
            "--output-dir",
            "output",
            "extra",
        ]))
        .unwrap_err();

        assert_eq!(error, "unknown argument: extra");
    }
}
