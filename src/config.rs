use std::{fmt, net::SocketAddr, num::NonZeroU32, str::FromStr};

use clap::{ArgAction, Parser, ValueEnum, builder::BoolishValueParser};

#[derive(Debug, Parser)]
#[command(name = "memkafka", version, about)]
pub struct Cli {
    #[arg(long, default_value = "127.0.0.1:9092")]
    kafka_listen: SocketAddr,

    #[arg(long)]
    kafka_advertised_address: Option<String>,

    #[arg(long, default_value = "127.0.0.1:8081")]
    schema_registry_listen: SocketAddr,

    #[arg(
        long,
        default_value_t = true,
        action = ArgAction::Set,
        value_parser = BoolishValueParser::new()
    )]
    auto_create_topics: bool,

    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u32).range(1..))]
    default_partitions: u32,

    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    log_level: LogLevel,

    #[arg(long)]
    quiet: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvertisedAddress {
    host: String,
    port: u16,
}

impl AdvertisedAddress {
    pub fn new(host: impl Into<String>, port: u16) -> Result<Self, ConfigError> {
        let host = host.into();
        let host = host.trim();

        if host.is_empty() {
            return Err(ConfigError::new("advertised host must not be empty"));
        }
        if port == 0 {
            return Err(ConfigError::new(
                "advertised port must be greater than zero",
            ));
        }

        Ok(Self {
            host: host.to_owned(),
            port,
        })
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for AdvertisedAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(formatter, "[{}]:{}", self.host, self.port)
        } else {
            write!(formatter, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for AdvertisedAddress {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parsed = if let Some(without_opening_bracket) = value.strip_prefix('[') {
            without_opening_bracket.rsplit_once("]:")
        } else {
            value
                .rsplit_once(':')
                .and_then(|(host, port)| (!host.contains(':')).then_some((host, port)))
        };

        let Some((host, port)) = parsed else {
            return Err(ConfigError::invalid_advertised_address(
                value,
                "expected host:port",
            ));
        };
        let port = port.parse::<u16>().map_err(|_| {
            ConfigError::invalid_advertised_address(
                value,
                "port must be an integer from 1 to 65535",
            )
        })?;

        Self::new(host, port)
            .map_err(|error| ConfigError::invalid_advertised_address(value, error.message))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub broker_id: i32,
    pub kafka_listen: SocketAddr,
    pub kafka_advertised_address: Option<AdvertisedAddress>,
    pub schema_registry_listen: SocketAddr,
    pub auto_create_topics: bool,
    pub default_partitions: NonZeroU32,
    pub log_level: LogLevel,
    pub quiet: bool,
}

impl TryFrom<Cli> for Config {
    type Error = ConfigError;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let kafka_advertised_address = cli
            .kafka_advertised_address
            .as_deref()
            .map(AdvertisedAddress::from_str)
            .transpose()?;

        Ok(Self {
            broker_id: 1,
            kafka_listen: cli.kafka_listen,
            kafka_advertised_address,
            schema_registry_listen: cli.schema_registry_listen,
            auto_create_topics: cli.auto_create_topics,
            default_partitions: NonZeroU32::new(cli.default_partitions)
                .expect("clap rejects a zero partition count"),
            log_level: cli.log_level,
            quiet: cli.quiet,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigError {
    message: String,
}

impl ConfigError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn invalid_advertised_address(value: &str, reason: impl fmt::Display) -> Self {
        Self::new(format!(
            "invalid Kafka advertised address '{value}': {reason}"
        ))
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn defaults_match_the_public_contract() {
        let config = Config::try_from(Cli::try_parse_from(["memkafka"]).unwrap()).unwrap();

        assert_eq!(config.kafka_listen, "127.0.0.1:9092".parse().unwrap());
        assert_eq!(
            config.schema_registry_listen,
            "127.0.0.1:8081".parse().unwrap()
        );
        assert_eq!(config.kafka_advertised_address, None);
        assert!(config.auto_create_topics);
        assert_eq!(config.default_partitions.get(), 2);
        assert_eq!(config.log_level, LogLevel::Info);
        assert!(!config.quiet);
    }

    #[test]
    fn zero_default_partitions_is_rejected() {
        let error = Cli::try_parse_from(["memkafka", "--default-partitions", "0"]).unwrap_err();

        assert!(error.to_string().contains("--default-partitions"));
    }

    #[test]
    fn advertised_address_accepts_a_dns_name() {
        let config = Config::try_from(
            Cli::try_parse_from(["memkafka", "--kafka-advertised-address", "broker:19092"])
                .unwrap(),
        )
        .unwrap();

        assert_eq!(
            config.kafka_advertised_address,
            Some(AdvertisedAddress::new("broker", 19092).unwrap())
        );
    }

    #[test]
    fn advertised_address_rejects_a_missing_port() {
        let error = Config::try_from(
            Cli::try_parse_from(["memkafka", "--kafka-advertised-address", "broker"]).unwrap(),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid Kafka advertised address 'broker': expected host:port"
        );
    }
}
