use std::{future::Future, net::SocketAddr};

use anyhow::{Context, Result, anyhow};
use axum::Router;
use tokio::{
    net::TcpListener,
    sync::{oneshot, watch},
    task::{JoinError, JoinSet},
};
use tracing::{debug, info, warn};

use crate::{
    config::{AdvertisedAddress, Config},
    kafka::{connection, dispatcher::Dispatcher},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundEndpoints {
    pub kafka: SocketAddr,
    pub schema_registry: SocketAddr,
    pub advertised_kafka: AdvertisedAddress,
}

impl BoundEndpoints {
    pub fn new(
        kafka: SocketAddr,
        schema_registry: SocketAddr,
        advertised_kafka: AdvertisedAddress,
    ) -> Self {
        Self {
            kafka,
            schema_registry,
            advertised_kafka,
        }
    }
}

pub fn readiness_message(endpoints: &BoundEndpoints) -> String {
    format!(
        "MemKafka ready kafka={} schema_registry=http://{} advertised_kafka={}",
        endpoints.kafka, endpoints.schema_registry, endpoints.advertised_kafka
    )
}

pub async fn serve<F>(
    config: Config,
    ready: oneshot::Sender<BoundEndpoints>,
    shutdown: F,
) -> Result<()>
where
    F: Future<Output = ()> + Send,
{
    let kafka_listener = TcpListener::bind(config.kafka_listen)
        .await
        .with_context(|| format!("failed to bind Kafka listener at {}", config.kafka_listen))?;
    let schema_registry_listener = TcpListener::bind(config.schema_registry_listen)
        .await
        .with_context(|| {
            format!(
                "failed to bind Schema Registry listener at {}",
                config.schema_registry_listen
            )
        })?;

    let kafka_address = kafka_listener
        .local_addr()
        .context("failed to read bound Kafka listener address")?;
    let schema_registry_address = schema_registry_listener
        .local_addr()
        .context("failed to read bound Schema Registry listener address")?;
    let advertised_kafka = config.kafka_advertised_address.unwrap_or(
        AdvertisedAddress::new(kafka_address.ip().to_string(), kafka_address.port())
            .context("failed to derive Kafka advertised address")?,
    );
    let endpoints = BoundEndpoints::new(kafka_address, schema_registry_address, advertised_kafka);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut servers = JoinSet::new();
    servers.spawn(run_kafka_listener(kafka_listener, shutdown_rx.clone()));
    servers.spawn(run_schema_registry_listener(
        schema_registry_listener,
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
    while let Some(completed) = servers.join_next().await {
        if result.is_ok() {
            result = completed
                .map_err(join_error_to_anyhow)
                .and_then(|task| task);
        }
    }

    result
}

async fn run_kafka_listener(
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let dispatcher = Dispatcher;
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
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    axum::serve(listener, Router::new())
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
