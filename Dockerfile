FROM rust:1.98.0-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime

RUN groupadd --system memkafka \
    && useradd --system --gid memkafka --no-create-home memkafka

COPY --from=builder --chown=memkafka:memkafka /build/target/release/memkafka /usr/local/bin/memkafka

USER memkafka

EXPOSE 9092 8081

ENTRYPOINT ["/usr/local/bin/memkafka"]
CMD ["--kafka-listen", "0.0.0.0:9092", "--schema-registry-listen", "0.0.0.0:8081", "--kafka-advertised-address", "localhost:9092"]

