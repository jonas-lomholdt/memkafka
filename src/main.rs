use std::process::ExitCode;

use clap::Parser;
use memkafka::{
    config::{Cli, Config},
    logging,
    server::serve,
};
use tokio::sync::oneshot;

#[tokio::main]
async fn main() -> ExitCode {
    let config = Config::from(Cli::parse());

    if let Err(error) = logging::init(config.log_level, config.quiet) {
        return fatal(error);
    }

    let (ready_tx, _ready_rx) = oneshot::channel();
    let shutdown = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to listen for the shutdown signal");
        }
    };

    match serve(config, ready_tx, shutdown).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => fatal(error),
    }
}

fn fatal(error: impl std::fmt::Display) -> ExitCode {
    eprintln!("memkafka: {error}");
    ExitCode::FAILURE
}
