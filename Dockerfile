FROM node:26.11.1-trixie-slim AS frontend

WORKDIR /ui

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY ui/package.json ui/package-lock.json* ./

RUN --mount=type=cache,target=/root/.npm \
  npm ci

COPY ui/ ./

RUN npm run build

# Keep the Rust tag on the toolchain pinned in .github/workflows/ci.yml.
FROM rust:1.99.0-slim-trixie AS chef

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  libssl-dev \
  pkg-config \
  && rm -rf /var/lib/apt/lists/*

RUN cargo install --locked cargo-chef --version 0.1.78

WORKDIR /usr/src/fitz

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS backend

COPY --from=planner /usr/src/fitz/recipe.json recipe.json

RUN --mount=type=cache,target=/usr/local/cargo/registry \
  cargo chef cook --release --locked --recipe-path recipe.json

COPY . .

# cargo-chef primes the target dir with a placeholder binary for dependency caching.
# Remove that stub so the final image always contains a binary built from the real sources.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  rm -f target/release/fitz target/release/deps/fitz* \
  && cargo build --release --locked --bin fitz \
  && (strip target/release/fitz || true)

FROM debian:trixie-slim AS runtime-fs

RUN mkdir -p /data \
  && chown 65532:65532 /data

FROM gcr.io/distroless/cc-debian13:latest AS runtime

WORKDIR /app

ENV FITZ_HTTP_PORT=4090 \
  FITZ_TCP_PORT=4091 \
  FITZ_METRICS_BIND_ADDR=0.0.0.0 \
  FITZ_METRICS_PORT=9090

COPY --from=runtime-fs --chown=65532:65532 /data /data
COPY --from=backend /usr/src/fitz/target/release/fitz /app/fitz
COPY --from=frontend --chown=65532:65532 /ui/dist/ /app/public/

USER 65532:65532

EXPOSE ${FITZ_HTTP_PORT} ${FITZ_TCP_PORT} ${FITZ_METRICS_PORT}

VOLUME ["/data"]

ENTRYPOINT ["/app/fitz"]
