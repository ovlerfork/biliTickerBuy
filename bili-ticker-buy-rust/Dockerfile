FROM rust:1-bookworm AS api
WORKDIR /app/src-tauri
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
COPY src-tauri/Cargo.toml src-tauri/Cargo.lock src-tauri/build.rs src-tauri/tauri.conf.json ./
COPY src-tauri/icons ./icons
COPY src-tauri/src ./src
COPY dist ../dist
RUN cargo build --release --no-default-features --features web --bin web

FROM debian:bookworm-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data
COPY --from=api /app/src-tauri/target/release/web /usr/local/bin/bili-ticker-buy-web
COPY dist ./dist
ENV PORT=8080 WEB_DIST_DIR=/app/dist APP_DATA_DIR=/data
EXPOSE 8080
VOLUME ["/data"]
CMD ["bili-ticker-buy-web"]
