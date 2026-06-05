FROM node:slim AS frontend

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  bash \
  ca-certificates \
  curl \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://vite.plus | bash

ENV PATH="/root/.vite-plus/bin:${PATH}"

WORKDIR /ui

COPY ui/package.json ui/package-lock.json* ./

RUN --mount=type=cache,target=/root/.npm \
  npm ci

COPY ui/ ./

RUN npm run build

FROM rust:slim AS chef

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  libssl-dev \
  pkg-config \
  && rm -rf /var/lib/apt/lists/*

RUN cargo install --locked cargo-chef

WORKDIR /usr/src/fitz

FROM chef AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS backend

COPY --from=planner /usr/src/fitz/recipe.json recipe.json

RUN --mount=type=cache,target=/usr/local/cargo/registry \
  cargo chef cook --release --locked --recipe-path recipe.json

COPY . .
COPY --from=frontend /ui/dist/ /usr/src/fitz/embedded-ui/

ENV FITZ_EMBED_UI_DIR=/usr/src/fitz/embedded-ui

RUN --mount=type=cache,target=/usr/local/cargo/registry \
  cargo build --release --locked \
  && strip target/release/fitz || true

FROM debian:trixie-slim AS runtime-fs

RUN mkdir -p /data \
  && chown 65532:65532 /data

FROM gcr.io/distroless/cc-debian13 AS runtime

WORKDIR /app

ENV FITZ_HTTP_PORT=4090 \
  FITZ_TCP_PORT=4091

COPY --from=runtime-fs --chown=65532:65532 /data /data
COPY --from=backend /usr/src/fitz/target/release/fitz /app/fitz

USER 65532:65532

EXPOSE ${FITZ_HTTP_PORT} ${FITZ_TCP_PORT}

VOLUME ["/data"]

ENTRYPOINT ["/app/fitz"]