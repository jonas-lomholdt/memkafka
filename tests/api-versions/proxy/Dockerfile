FROM rust:1.98.0-bookworm AS builder

WORKDIR /build

COPY rust-toolchain.toml ./
COPY tests/api-versions/proxy/Cargo.toml tests/api-versions/proxy/Cargo.lock ./tests/api-versions/proxy/
COPY tests/api-versions/proxy/src ./tests/api-versions/proxy/src

RUN cargo build --locked --release --manifest-path tests/api-versions/proxy/Cargo.toml

FROM debian:bookworm-slim AS runtime

COPY --from=builder /build/tests/api-versions/proxy/target/release/kafka-api-version-proxy /usr/local/bin/kafka-api-version-proxy

ENTRYPOINT ["/usr/local/bin/kafka-api-version-proxy"]
