FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Build dependencies first so they are cached between code changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && cargo build --release && rm -rf src
COPY src ./src
RUN touch src/main.rs && cargo build --release && cp target/release/orders /app

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* && useradd --uid 10001 app
USER app
COPY --from=build /app /app
EXPOSE 8080
ENTRYPOINT ["/app"]
