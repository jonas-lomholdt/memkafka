use std::{future::Future, net::SocketAddr, time::Duration};

use anyhow::{Context, Result, anyhow};
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
    task::{JoinError, JoinSet},
    time::timeout,
};
use tracing::{debug, info, warn};

use crate::{
    broker::BrokerState,
    config::{AdvertisedAddress, Config, KafkaListener},
    kafka::{connection, dispatcher::Dispatcher},
    schema_registry::{Registry, router as schema_registry_router},
};

const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundKafkaListener {
    listen: SocketAddr,
    advertised: AdvertisedAddress,
}

impl BoundKafkaListener {
    pub fn new(listen: SocketAddr, advertised: AdvertisedAddress) -> Self {
        Self { listen, advertised }
    }

    pub fn listen(&self) -> SocketAddr {
        self.listen
    }

    pub fn advertised(&self) -> &AdvertisedAddress {
        &self.advertised
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundEndpoints {
    kafka: Vec<BoundKafkaListener>,
    schema_registry: SocketAddr,
}

impl BoundEndpoints {
    pub fn new(kafka: Vec<BoundKafkaListener>, schema_registry: SocketAddr) -> Self {
        Self {
            kafka,
            schema_registry,
        }
    }

    pub fn kafka_listeners(&self) -> &[BoundKafkaListener] {
        &self.kafka
    }

    pub fn primary_kafka(&self) -> &BoundKafkaListener {
        self.kafka
            .first()
            .expect("MemKafka binds at least one Kafka listener")
    }

    pub fn kafka(&self) -> SocketAddr {
        self.primary_kafka().listen()
    }

    pub fn advertised_kafka(&self) -> &AdvertisedAddress {
        self.primary_kafka().advertised()
    }

    pub fn schema_registry(&self) -> SocketAddr {
        self.schema_registry
    }
}

pub fn readiness_message(endpoints: &BoundEndpoints) -> String {
    format!(
        "MemKafka ready kafka={} schema_registry=http://{} advertised_kafka={}",
        joined(endpoints, |listener| listener.listen().to_string()),
        endpoints.schema_registry,
        joined(endpoints, |listener| listener.advertised().to_string())
    )
}

fn joined(endpoints: &BoundEndpoints, describe: impl Fn(&BoundKafkaListener) -> String) -> String {
    endpoints
        .kafka_listeners()
        .iter()
        .map(describe)
        .collect::<Vec<_>>()
        .join(",")
}

pub async fn serve<F>(
    config: Config,
    ready: oneshot::Sender<BoundEndpoints>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send,
{
    if config.kafka_listeners.is_empty() {
        return Err(anyhow!("at least one Kafka listener is required"));
    }

    let mut kafka_listeners = Vec::with_capacity(config.kafka_listeners.len());
    for listener_config in &config.kafka_listeners {
        kafka_listeners.push(bind_kafka_listener(listener_config).await?);
    }
    let schema_registry_listener = TcpListener::bind(config.schema_registry_listen)
        .await
        .with_context(|| {
            format!(
                "failed to bind Schema Registry listener at {}",
                config.schema_registry_listen
            )
        })?;

    let schema_registry_address = schema_registry_listener
        .local_addr()
        .context("failed to read bound Schema Registry listener address")?;
    let endpoints = BoundEndpoints::new(
        kafka_listeners
            .iter()
            .map(|(_, bound)| bound.clone())
            .collect(),
        schema_registry_address,
    );
    let broker = BrokerState::new(
        config.broker_id,
        config.auto_create_topics,
        config.force_auto_create_topics,
        config.default_partitions,
    );

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut servers = JoinSet::new();
    for (listener, bound) in kafka_listeners {
        servers.spawn(run_kafka_listener(
            listener,
            Dispatcher::new(broker.clone(), bound.advertised().clone()),
            shutdown_rx.clone(),
        ));
    }
    servers.spawn(run_schema_registry_listener(
        schema_registry_listener,
        Registry::new(),
        shutdown_rx,
    ));

    info!("{}", readiness_message(&endpoints));
    let _ = ready.send(endpoints);

    tokio::pin!(shutdown);
    let mut result = tokio::select! {
        () = &mut shutdown => Ok(()),
        completed = servers.join_next() => unexpected_task_result(completed),
    };

    let _ = shutdown_tx.send(true);
    let mut drain_result = Ok(());
    if timeout(
        SHUTDOWN_GRACE_PERIOD,
        drain_servers(&mut servers, &mut drain_result),
    )
    .await
    .is_err()
    {
        warn!(
            grace_period_ms = SHUTDOWN_GRACE_PERIOD.as_millis(),
            "server shutdown grace period elapsed; aborting remaining tasks"
        );
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }
    if result.is_ok() {
        result = drain_result;
    }

    result
}

async fn bind_kafka_listener(
    listener_config: &KafkaListener,
) -> Result<(TcpListener, BoundKafkaListener)> {
    let listener = TcpListener::bind(listener_config.listen)
        .await
        .with_context(|| {
            format!(
                "failed to bind Kafka listener at {}",
                listener_config.listen
            )
        })?;
    let address = listener
        .local_addr()
        .context("failed to read bound Kafka listener address")?;
    let advertised = match &listener_config.advertised {
        Some(advertised) => advertised.clone(),
        None => AdvertisedAddress::new(address.ip().to_string(), address.port())
            .context("failed to derive Kafka advertised address")?,
    };

    Ok((listener, BoundKafkaListener::new(address, advertised)))
}

async fn drain_servers(servers: &mut JoinSet<Result<()>>, result: &mut Result<()>) {
    while let Some(completed) = servers.join_next().await {
        if result.is_ok() {
            *result = completed
                .map_err(join_error_to_anyhow)
                .and_then(|task| task);
        }
    }
}

async fn run_kafka_listener(
    listener: TcpListener,
    dispatcher: Dispatcher,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut connections = JoinSet::new();

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (socket, peer) = accepted.context("Kafka listener failed to accept a connection")?;
                debug!(%peer, "accepted Kafka connection");
                let dispatcher = dispatcher.clone();
                let connection_shutdown = shutdown.clone();
                connections.spawn(async move {
                    (peer, connection::serve(socket, dispatcher, connection_shutdown).await)
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(completed);
            }
        }
    }

    while let Some(completed) = connections.join_next().await {
        log_connection_result(Some(completed));
    }
    Ok(())
}

fn log_connection_result(completed: Option<Result<(SocketAddr, Result<()>), JoinError>>) {
    match completed {
        Some(Ok((peer, Ok(())))) => debug!(%peer, "Kafka connection closed"),
        Some(Ok((peer, Err(error)))) => warn!(%peer, %error, "Kafka connection failed"),
        Some(Err(error)) => warn!(%error, "Kafka connection task failed"),
        None => {}
    }
}

async fn run_schema_registry_listener(
    listener: TcpListener,
    registry: Registry,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    axum::serve(listener, schema_registry_router(registry))
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await
        .context("Schema Registry server failed")
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }

    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow_and_update() {
            return;
        }
    }
}

fn unexpected_task_result(completed: Option<Result<Result<()>, JoinError>>) -> Result<()> {
    match completed {
        Some(Ok(Ok(()))) => Err(anyhow!("listener task stopped before shutdown")),
        Some(Ok(Err(error))) => Err(error),
        Some(Err(error)) => Err(join_error_to_anyhow(error)),
        None => Err(anyhow!("all listener tasks stopped before shutdown")),
    }
}

fn join_error_to_anyhow(error: JoinError) -> anyhow::Error {
    anyhow!("listener task failed: {error}")
}
