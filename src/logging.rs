use anyhow::{Result, anyhow};
use tracing_subscriber::EnvFilter;

use crate::config::LogLevel;

pub fn init(level: LogLevel, quiet: bool) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter_directive(level, quiet)))
        .with_target(false)
        .try_init()
        .map_err(|error| anyhow!("failed to initialize logging: {error}"))
}

fn filter_directive(level: LogLevel, quiet: bool) -> &'static str {
    if quiet {
        return "warn";
    }

    match level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LogLevel;

    #[test]
    fn quiet_caps_ordinary_logging_at_warn() {
        assert_eq!(filter_directive(LogLevel::Trace, true), "warn");
    }

    #[test]
    fn selected_level_is_used_when_not_quiet() {
        assert_eq!(filter_directive(LogLevel::Debug, false), "debug");
    }
}
