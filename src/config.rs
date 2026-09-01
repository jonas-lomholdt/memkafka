use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroU32,
    str::FromStr,
};

use clap::{ArgAction, Parser, ValueEnum, builder::BoolishValueParser};

const DEFAULT_KAFKA_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9092);

#[derive(Debug, Parser)]
#[command(name = "memkafka", version, about)]
pub struct Cli {
    /// Kafka listener as `listen=<host:port>[,advertised=<host:port>]`. Repeat for one listener per
    /// network. Cannot be combined with --kafka-listen or --kafka-advertised-address.
    #[arg(
        long,
        value_name = "FIELDS",
        conflicts_with_all = ["kafka_listen", "kafka_advertised_address"]
    )]
    kafka_listener: Vec<KafkaListener>,

    #[arg(long, default_value_t = DEFAULT_KAFKA_LISTEN)]
    kafka_listen: SocketAddr,

    #[arg(long)]
    kafka_advertised_address: Option<AdvertisedAddress>,

    #[arg(long, default_value = "127.0.0.1:8081")]
    schema_registry_listen: SocketAddr,

    #[arg(
        long,
        default_value_t = true,
        action = ArgAction::Set,
        value_parser = BoolishValueParser::new()
    )]
    auto_create_topics: bool,

    #[arg(
        long,
        default_value_t = false,
        action = ArgAction::Set,
        value_parser = BoolishValueParser::new()
    )]
    force_auto_create_topics: bool,

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
pub struct KafkaListener {
    pub listen: SocketAddr,
    pub advertised: Option<AdvertisedAddress>,
}

impl KafkaListener {
    pub fn new(listen: SocketAddr, advertised: Option<AdvertisedAddress>) -> Self {
        Self { listen, advertised }
    }
}

const LISTEN_FIELD: &str = "listen";
const ADVERTISED_FIELD: &str = "advertised";

impl FromStr for KafkaListener {
    type Err = ConfigError;

    /// Parses `listen=<host:port>[,advertised=<host:port>]`. Fields are order-independent, and an
    /// omitted `advertised` means the listener advertises its own bound address.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut listen = None;
        let mut advertised = None;

        for field in value.split(',') {
            let field = field.trim();
            let Some((key, raw)) = field.split_once('=') else {
                return Err(ConfigError::invalid_listener(
                    value,
                    format!("expected {LISTEN_FIELD}=<host:port> fields, got '{field}'"),
                ));
            };

            let raw = raw.trim();
            match key.trim() {
                LISTEN_FIELD => {
                    if listen.is_some() {
                        return Err(ConfigError::duplicate_listener_field(value, LISTEN_FIELD));
                    }
                    listen = Some(raw.parse::<SocketAddr>().map_err(|_| {
                        ConfigError::invalid_listener(
                            value,
                            format!("{LISTEN_FIELD} '{raw}' must be an address literal host:port"),
                        )
                    })?);
                }
                ADVERTISED_FIELD => {
                    if advertised.is_some() {
                        return Err(ConfigError::duplicate_listener_field(
                            value,
                            ADVERTISED_FIELD,
                        ));
                    }
                    advertised = Some(AdvertisedAddress::from_str(raw)?);
                }
                unknown => {
                    return Err(ConfigError::invalid_listener(
                        value,
                        format!(
                            "unknown field '{unknown}', expected {LISTEN_FIELD} or {ADVERTISED_FIELD}"
                        ),
                    ));
                }
            }
        }

        let listen = listen.ok_or_else(|| {
            ConfigError::invalid_listener(value, format!("missing required {LISTEN_FIELD} field"))
        })?;

        Ok(Self::new(listen, advertised))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub broker_id: i32,
    pub kafka_listeners: Vec<KafkaListener>,
    pub schema_registry_listen: SocketAddr,
    pub auto_create_topics: bool,
    pub force_auto_create_topics: bool,
    pub default_partitions: NonZeroU32,
    pub log_level: LogLevel,
    pub quiet: bool,
}

impl From<Cli> for Config {
    fn from(cli: Cli) -> Self {
        // --kafka-listener carries every listener explicitly. The older single-listener
        // --kafka-listen / --kafka-advertised-address pair keeps working unchanged; clap rejects
        // combining the two styles.
        let kafka_listeners = if cli.kafka_listener.is_empty() {
            let listen = cli.kafka_listen;
            let advertised = cli.kafka_advertised_address;

            vec![KafkaListener::new(listen, advertised)]
        } else {
            cli.kafka_listener
        };

        Self {
            broker_id: 1,
            kafka_listeners,
            schema_registry_listen: cli.schema_registry_listen,
            auto_create_topics: cli.auto_create_topics,
            force_auto_create_topics: cli.force_auto_create_topics,
            default_partitions: NonZeroU32::new(cli.default_partitions)
                .expect("clap rejects a zero partition count"),
            log_level: cli.log_level,
            quiet: cli.quiet,
        }
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

    fn invalid_listener(value: &str, reason: impl fmt::Display) -> Self {
        Self::new(format!("invalid Kafka listener '{value}': {reason}"))
    }

    fn duplicate_listener_field(value: &str, field: &str) -> Self {
        Self::new(format!(
            "invalid Kafka listener '{value}': {field} is set more than once"
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
    use clap::{Parser, error::ErrorKind};

    #[test]
    fn defaults_match_the_public_contract() {
        let config = Config::from(Cli::try_parse_from(["memkafka"]).unwrap());

        assert_eq!(
            config.kafka_listeners,
            vec![KafkaListener::new("127.0.0.1:9092".parse().unwrap(), None)]
        );
        assert_eq!(
            config.schema_registry_listen,
            "127.0.0.1:8081".parse().unwrap()
        );
        assert!(config.auto_create_topics);
        assert!(!config.force_auto_create_topics);
        assert_eq!(config.default_partitions.get(), 2);
        assert_eq!(config.log_level, LogLevel::Info);
        assert!(!config.quiet);
    }

    #[test]
    fn help_reports_the_legacy_kafka_listener_default() {
        let error = Cli::try_parse_from(["memkafka", "--help"]).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        let help = error.to_string();
        let kafka_listen_help = help
            .split_once("--kafka-listen <KAFKA_LISTEN>")
            .expect("help includes --kafka-listen")
            .1
            .split_once("--kafka-advertised-address")
            .expect("--kafka-advertised-address follows --kafka-listen")
            .0;

        assert!(
            kafka_listen_help.contains("[default: 127.0.0.1:9092]"),
            "--kafka-listen help did not report its default: {kafka_listen_help}"
        );
    }

    #[test]
    fn force_auto_create_topics_accepts_an_explicit_true_value() {
        let config = Config::from(
            Cli::try_parse_from(["memkafka", "--force-auto-create-topics", "true"]).unwrap(),
        );

        assert!(config.force_auto_create_topics);
    }

    #[test]
    fn zero_default_partitions_is_rejected() {
        let error = Cli::try_parse_from(["memkafka", "--default-partitions", "0"]).unwrap_err();

        assert!(error.to_string().contains("--default-partitions"));
    }

    #[test]
    fn advertised_address_accepts_a_dns_name() {
        let config = Config::from(
            Cli::try_parse_from(["memkafka", "--kafka-advertised-address", "broker:19092"])
                .unwrap(),
        );

        assert_eq!(
            config.kafka_listeners,
            vec![KafkaListener::new(
                "127.0.0.1:9092".parse().unwrap(),
                Some(AdvertisedAddress::new("broker", 19092).unwrap())
            )]
        );
    }

    #[test]
    fn a_listener_pairs_its_own_advertised_address() {
        let config = Config::from(
            Cli::try_parse_from([
                "memkafka",
                "--kafka-listener",
                "listen=0.0.0.0:9092,advertised=localhost:9092",
                "--kafka-listener",
                "listen=0.0.0.0:9093,advertised=kafka:9093",
            ])
            .unwrap(),
        );

        assert_eq!(
            config.kafka_listeners,
            vec![
                KafkaListener::new(
                    "0.0.0.0:9092".parse().unwrap(),
                    Some(AdvertisedAddress::new("localhost", 9092).unwrap())
                ),
                KafkaListener::new(
                    "0.0.0.0:9093".parse().unwrap(),
                    Some(AdvertisedAddress::new("kafka", 9093).unwrap())
                ),
            ]
        );
    }

    #[test]
    fn listener_fields_are_order_independent() {
        let config = Config::from(
            Cli::try_parse_from([
                "memkafka",
                "--kafka-listener",
                "advertised=kafka:9093, listen=0.0.0.0:9093",
            ])
            .unwrap(),
        );

        assert_eq!(
            config.kafka_listeners,
            vec![KafkaListener::new(
                "0.0.0.0:9093".parse().unwrap(),
                Some(AdvertisedAddress::new("kafka", 9093).unwrap())
            )]
        );
    }

    #[test]
    fn a_listener_without_an_advertised_field_derives_its_own_address() {
        let config = Config::from(
            Cli::try_parse_from(["memkafka", "--kafka-listener", "listen=0.0.0.0:9092"]).unwrap(),
        );

        assert_eq!(
            config.kafka_listeners,
            vec![KafkaListener::new("0.0.0.0:9092".parse().unwrap(), None)]
        );
    }

    #[test]
    fn a_listener_without_a_listen_field_is_rejected() {
        let error = Cli::try_parse_from(["memkafka", "--kafka-listener", "advertised=kafka:9093"])
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("missing required listen field"));
    }

    #[test]
    fn a_listener_with_an_unknown_field_is_rejected() {
        let error = Cli::try_parse_from([
            "memkafka",
            "--kafka-listener",
            "listen=0.0.0.0:9092,name=HOST",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("unknown field 'name'"));
    }

    #[test]
    fn a_listener_repeating_the_listen_field_is_rejected() {
        let error = Cli::try_parse_from([
            "memkafka",
            "--kafka-listener",
            "listen=0.0.0.0:9092,listen=0.0.0.0:9093",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("listen is set more than once"));
    }

    #[test]
    fn a_listener_repeating_the_advertised_field_is_rejected() {
        let error = Cli::try_parse_from([
            "memkafka",
            "--kafka-listener",
            "listen=0.0.0.0:9092,advertised=host:9092,advertised=container:9093",
        ])
        .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(
            error
                .to_string()
                .contains("advertised is set more than once")
        );
    }

    #[test]
    fn a_listener_field_without_a_value_is_rejected() {
        let error =
            Cli::try_parse_from(["memkafka", "--kafka-listener", "0.0.0.0:9092"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("expected listen=<host:port>"));
    }

    #[test]
    fn each_legacy_listener_option_conflicts_with_the_multi_listener_style() {
        for (legacy_option, legacy_value) in [
            ("--kafka-listen", "0.0.0.0:9093"),
            ("--kafka-advertised-address", "legacy:9093"),
        ] {
            let error = Cli::try_parse_from([
                "memkafka",
                "--kafka-listener",
                "listen=0.0.0.0:9092",
                legacy_option,
                legacy_value,
            ])
            .unwrap_err();
            let rendered = error.to_string();

            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
            assert!(rendered.contains("--kafka-listener"));
            assert!(rendered.contains(legacy_option));
        }
    }

    #[test]
    fn advertised_address_rejects_a_missing_port() {
        let error =
            Cli::try_parse_from(["memkafka", "--kafka-advertised-address", "broker"]).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::ValueValidation);
        assert!(error.to_string().contains("expected host:port"));
    }
}
